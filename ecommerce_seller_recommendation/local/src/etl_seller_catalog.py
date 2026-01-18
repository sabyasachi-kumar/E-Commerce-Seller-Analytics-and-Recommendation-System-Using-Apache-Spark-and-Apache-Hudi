#!/usr/bin/env python3

import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, initcap, when, lit, current_timestamp
)
from pyspark.sql.types import DoubleType, IntegerType, StringType


# -------------------------------------------------------
# Load YAML config
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
# Cleaning (Logical Silver)
# -------------------------------------------------------
def clean_seller_catalog(df):
    """
    Apply cleaning transformations to seller catalog data.

    Cleaning steps:
    - Trim whitespace from string columns
    - Apply title case to item_name and category
    - Cast marketplace_price to DOUBLE
    - Cast stock_qty to INT
    - Fill missing stock_qty with 0
    - Remove duplicates on (seller_id, item_id)

    Args:
        df: Raw DataFrame from Bronze layer

    Returns:
        Cleaned DataFrame (Silver layer)
    """
    df = (
        df
        .withColumn("seller_id", trim(col("seller_id")).cast(StringType()))
        .withColumn("item_id", trim(col("item_id")).cast(StringType()))
        .withColumn("item_name", initcap(trim(col("item_name"))))
        .withColumn("category", initcap(trim(col("category"))))
        .withColumn("marketplace_price", col("marketplace_price").cast(DoubleType()))
        .withColumn("stock_qty", col("stock_qty").cast(IntegerType()))
    )

    df = df.withColumn(
        "stock_qty",
        when(col("stock_qty").isNull(), lit(0)).otherwise(col("stock_qty"))
    )

    df = df.dropDuplicates(["seller_id", "item_id"])

    return df


# -------------------------------------------------------
# DQ Checks
# -------------------------------------------------------
def dq_split(df_clean):
    """
    Apply data quality checks and split records into valid and invalid sets.

    DQ Rules:
    - seller_id, item_id, item_name, category must NOT be NULL
    - marketplace_price must be NOT NULL and >= 0
    - stock_qty must be NOT NULL and >= 0

    Args:
        df_clean: Cleaned DataFrame from Silver layer

    Returns:
        tuple: (valid_df, invalid_df)
            - valid_df: Records passing all DQ checks (Gold candidate)
            - invalid_df: Records failing DQ checks with failure reason (Quarantine)
    """
    condition = (
        col("seller_id").isNotNull() &
        col("item_id").isNotNull() &
        col("item_name").isNotNull() &
        col("category").isNotNull() &
        col("marketplace_price").isNotNull() & (col("marketplace_price") >= 0) &
        col("stock_qty").isNotNull() & (col("stock_qty") >= 0)
    )

    valid_df = df_clean.filter(condition)
    invalid_df = df_clean.filter(~condition)

    invalid_df = (
        invalid_df
        .withColumn(
            "dq_failure_reason",
            when(col("seller_id").isNull(), lit("missing_seller_id"))
            .when(col("item_id").isNull(), lit("missing_item_id"))
            .when(col("item_name").isNull(), lit("missing_item_name"))
            .when(col("category").isNull(), lit("missing_category"))
            .when(col("marketplace_price").isNull(), lit("missing_marketplace_price"))
            .when(col("marketplace_price") < 0, lit("negative_marketplace_price"))
            .when(col("stock_qty").isNull(), lit("missing_stock_qty"))
            .when(col("stock_qty") < 0, lit("negative_stock_qty"))
            .otherwise(lit("unknown_reason"))
        )
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("dataset_name", lit("seller_catalog"))
    )

    return valid_df, invalid_df


# -------------------------------------------------------
# Write Hudi (Gold Layer)
# -------------------------------------------------------
def write_hudi(df, output_path):
    """Write validated records to Hudi table (Gold layer)."""
    hudi_options = {
        "hoodie.table.name": "seller_catalog_table",
        "hoodie.datasource.write.recordkey.field": "seller_id,item_id",
        "hoodie.datasource.write.precombine.field": "ingestion_timestamp",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.schema.evolution.enable": "true",
        # IMPORTANT: do NOT set keygenerator.class here.
        # Hudi will infer the correct key generator from recordkey.field.
    }

    (
        df.write
        .format("hudi")
        .options(**hudi_options)
        .mode("overwrite")
        .save(output_path)
    )


# -------------------------------------------------------
# Write Quarantine
# -------------------------------------------------------
def write_quarantine(df, path):
    """Write invalid records to quarantine zone as CSV."""
    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .csv(path)
    )


# -------------------------------------------------------
# Argument parsing
# -------------------------------------------------------
def get_config_path(argv):
    """
    Support both of these:
    - spark-submit etl_seller_catalog.py ecomm_prod.yml
    - spark-submit etl_seller_catalog.py --config ecomm_prod.yml
    """
    if len(argv) == 2:
        return argv[1]
    if len(argv) == 3 and argv[1] == "--config":
        return argv[2]

    print("Usage: spark-submit etl_seller_catalog.py <config_path>")
    print("   or: spark-submit etl_seller_catalog.py --config <config_path>")
    sys.exit(1)


# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
def main():
    """
    Main ETL pipeline driver for Seller Catalog.

    Architecture:
    - BRONZE (logical): Raw CSV ingestion
    - SILVER (logical): In-memory cleaned DataFrame
    - GOLD (physical): DQ-validated records written to Hudi
    - QUARANTINE (physical): Invalid records written to CSV
    """
    try:
        config_path = get_config_path(sys.argv)

        config = load_config(config_path)

        input_path = config["seller_catalog"]["input_path"]
        hudi_output_path = config["seller_catalog"]["hudi_output_path"]

        # Example hudi_output_path:
        # /home/.../R1234/processed/seller_catalog_hudi/
        # project_root -> /home/.../R1234
        project_root = "/".join(hudi_output_path.rstrip("/").split("/")[:-2])
        quarantine_path = f"{project_root}/quarantine/seller_catalog/"

        spark = get_spark_session("ETL Seller Catalog")

        print(f"[INFO] Reading raw seller catalog (Bronze): {input_path}")
        df_raw = spark.read.option("header", True).csv(input_path)

        print("[INFO] Cleaning data (Silver)")
        df_clean = clean_seller_catalog(df_raw)

        print("[INFO] Applying DQ checks (Gold)")
        valid_df, invalid_df = dq_split(df_clean)

        print(f"[INFO] Valid rows: {valid_df.count()}")
        print(f"[INFO] Invalid rows: {invalid_df.count()}")

        # Add ingestion timestamp for Hudi precombine
        valid_df = valid_df.withColumn("ingestion_timestamp", current_timestamp())

        write_hudi(valid_df, hudi_output_path)
        print(f"[INFO] Wrote Hudi Gold layer: {hudi_output_path}")

        write_quarantine(invalid_df, quarantine_path)
        print(f"[INFO] Wrote Quarantine: {quarantine_path}")

        spark.stop()
        print("[SUCCESS] ETL Seller Catalog completed successfully")

    except Exception as e:
        print(f"[ERROR] ETL pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
