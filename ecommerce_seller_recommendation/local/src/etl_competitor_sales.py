#!/usr/bin/env python3

import sys
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    to_date,
    when,
    lit,
    current_timestamp,
    current_date,
)
from pyspark.sql.types import IntegerType, DoubleType, StringType


# -------------------------------------------------------
# Config loading
# -------------------------------------------------------
def load_config(path: str) -> dict:
    """Load YAML configuration file."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


# -------------------------------------------------------
# Spark Session
# -------------------------------------------------------
def get_spark_session(app_name: str) -> SparkSession:
    """Initialize and return Spark session with required configs."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# -------------------------------------------------------
# Argument parsing (same pattern as other ETLs)
# -------------------------------------------------------
def get_config_path(argv):
    """
    Support both of these:
    - spark-submit etl_competitor_sales.py ecomm_prod.yml
    - spark-submit etl_competitor_sales.py --config ecomm_prod.yml
    """
    if len(argv) == 2:
        return argv[1]
    if len(argv) == 3 and argv[1] == "--config":
        return argv[2]

    print("Usage: spark-submit etl_competitor_sales.py <config_path>")
    print("   or: spark-submit etl_competitor_sales.py --config <config_path>")
    sys.exit(1)


# -------------------------------------------------------
# Cleaning (Logical Silver)
# -------------------------------------------------------
def clean_competitor_sales(df):
    """
    Apply cleaning transformations to competitor sales data.

    Cleaning steps (per assignment):
    - Trim strings (item_id, seller_id, sale_date)
    - Normalize casing (we only trim IDs to stay consistent with other ETLs)
    - Cast numeric columns:
        units_sold        -> INT
        revenue           -> DOUBLE
        marketplace_price -> DOUBLE
    - Fill missing numeric fields (units_sold, revenue, marketplace_price) with 0
    - Convert sale_date -> DATE (yyyy-MM-dd) IN-PLACE

    NOTE: We DO NOT drop duplicates here to retain all logical rows for analysis.
    """

    df = (
        df
        .withColumn("item_id", trim(col("item_id")).cast(StringType()))
        .withColumn("seller_id", trim(col("seller_id")).cast(StringType()))
        .withColumn("sale_date", trim(col("sale_date")))
        .withColumn("units_sold", col("units_sold").cast(IntegerType()))
        .withColumn("revenue", col("revenue").cast(DoubleType()))
        .withColumn("marketplace_price", col("marketplace_price").cast(DoubleType()))
    )

    # Fill missing numeric fields with 0 (per assignment cleaning step)
    df = df.fillna(
        {
            "units_sold": 0,
            "revenue": 0.0,
            "marketplace_price": 0.0,
        }
    )

    # Parse sale_date into DATE, overwriting same column
    df = df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))

    return df


# -------------------------------------------------------
# DQ Checks + Split into Valid / Invalid
# -------------------------------------------------------
def dq_split(df_clean):
    """
    Apply data quality checks and split records into valid and invalid sets.

    DQ Rules (per assignment for competitor sales):
    - Item ID exists:           item_id IS NOT NULL
    - Seller ID exists:         seller_id IS NOT NULL
    - Units sold valid:         units_sold >= 0
    - Revenue valid:            revenue >= 0
    - Marketplace price valid:  marketplace_price >= 0
    - Sale date valid:          sale_date IS NOT NULL AND sale_date <= current_date()

    Returns:
        (valid_df, invalid_df)
    """

    item_id_valid = col("item_id").isNotNull()
    seller_id_valid = col("seller_id").isNotNull()
    units_valid = col("units_sold").isNotNull() & (col("units_sold") >= 0)
    revenue_valid = col("revenue").isNotNull() & (col("revenue") >= 0)
    price_valid = col("marketplace_price").isNotNull() & (col("marketplace_price") >= 0)
    date_valid = col("sale_date").isNotNull() & (col("sale_date") <= current_date())

    condition = (
        item_id_valid
        & seller_id_valid
        & units_valid
        & revenue_valid
        & price_valid
        & date_valid
    )

    valid_df = df_clean.filter(condition)
    invalid_df = df_clean.filter(~condition)

    invalid_df = (
        invalid_df
        .withColumn(
            "dq_failure_reason",
            when(~item_id_valid, lit("missing_item_id"))
            .when(~seller_id_valid, lit("missing_seller_id"))
            .when(~price_valid & col("marketplace_price").isNull(), lit("missing_marketplace_price"))
            .when(~price_valid & (col("marketplace_price") < 0), lit("negative_marketplace_price"))
            .when(~units_valid & col("units_sold").isNull(), lit("missing_units_sold"))
            .when(~units_valid & (col("units_sold") < 0), lit("negative_units_sold"))
            .when(~revenue_valid & col("revenue").isNull(), lit("missing_revenue"))
            .when(~revenue_valid & (col("revenue") < 0), lit("negative_revenue"))
            .when(~date_valid & col("sale_date").isNull(), lit("missing_sale_date"))
            .when(
                ~date_valid
                & col("sale_date").isNotNull()
                & (col("sale_date") > current_date()),
                lit("future_sale_date"),
            )
            .otherwise(lit("unknown_reason"))
        )
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("dataset_name", lit("competitor_sales"))
    )

    return valid_df, invalid_df


# -------------------------------------------------------
# Hudi Write (Gold)
# -------------------------------------------------------
def write_hudi(df, output_path: str):
    """
    Write validated records to Hudi table (Gold layer).

    - Record key:  seller_id,item_id
    - Precombine:  sale_date (latest sale_date wins for same key)
    - Operation:   upsert (idempotent writes)
    """

    hudi_options = {
        "hoodie.table.name": "competitor_sales_table",
        "hoodie.datasource.write.recordkey.field": "seller_id,item_id",
        "hoodie.datasource.write.precombine.field": "sale_date",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.schema.evolution.enable": "true",
        # Let Hudi infer key generator from recordkey.field
    }

    (
        df.write
        .format("hudi")
        .options(**hudi_options)
        # Assignment wants final Hudi tables with overwrite mode
        .mode("overwrite")
        .save(output_path)
    )

    print(f"[INFO] Hudi write complete → {output_path}")


# -------------------------------------------------------
# Quarantine Write
# -------------------------------------------------------
def write_quarantine(df, path: str):
    """
    Write invalid records to quarantine zone as CSV (physical quarantine).
    """
    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .csv(path)
    )
    print(f"[INFO] Quarantine write complete → {path}")


# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
def main():
    """
    Main ETL pipeline driver for Competitor Sales.

    Logical / Physical Architecture:
    - BRONZE (logical): Raw CSV ingestion -> df_raw
    - SILVER (logical): Cleaning (trim, cast, fill, date parse) -> df_clean
    - GOLD candidate (logical): DQ-validated data -> valid_df
    - GOLD (physical): Hudi table write for valid_df
    - QUARANTINE (physical): CSV write for invalid_df with dq_failure_reason
    """
    try:
        config_path = get_config_path(sys.argv)
        config = load_config(config_path)

        conf = config["competitor_sales"]
        input_path = conf["input_path"]
        hudi_output_path = conf["hudi_output_path"]

        # Derive project root from hudi_output_path:
        # Example: /home/.../R1234/processed/competitor_sales_hudi/
        # → project_root = /home/.../R1234
        parts = hudi_output_path.rstrip("/").split("/")
        if len(parts) < 3:
            raise ValueError(f"Unexpected hudi_output_path structure: {hudi_output_path}")
        project_root = "/".join(parts[:-2])
        quarantine_path = f"{project_root}/quarantine/competitor_sales/"

        spark = get_spark_session("ETL Competitor Sales")

        print(f"[INFO] Reading raw competitor sales (Bronze): {input_path}")
        df_raw = spark.read.option("header", True).csv(input_path)

        print("[INFO] Cleaning data (Silver)")
        df_clean = clean_competitor_sales(df_raw)

        print("[INFO] Applying DQ checks (Gold candidate split)")
        valid_df, invalid_df = dq_split(df_clean)

        print(f"[METRICS] RAW ROWS:          {df_raw.count()}")
        print(f"[METRICS] AFTER CLEAN:       {df_clean.count()}")
        print(f"[METRICS] VALID ROWS:        {valid_df.count()}")
        print(f"[METRICS] INVALID ROWS:      {invalid_df.count()}")

        # Add ingestion timestamp for traceability (even though precombine uses sale_date)
        valid_df = valid_df.withColumn("ingestion_timestamp", current_timestamp())

        write_hudi(valid_df, hudi_output_path)
        write_quarantine(invalid_df, quarantine_path)

        spark.stop()
        print("[SUCCESS] ETL Competitor Sales completed successfully")

    except Exception as e:
        print(f"[ERROR] ETL Competitor Sales pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
