#!/usr/bin/env python3

import sys
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg as spark_avg,
    countDistinct,
    first,
    when,
    lit,
    coalesce,
)

# -------------------------------------------------------
# Config helpers
# -------------------------------------------------------
def load_config(path: str) -> dict:
    """Load YAML configuration file."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_config_path(argv):
    """
    Support both of these:
    - spark-submit consumption_recommendation.py ecomm_prod.yml
    - spark-submit consumption_recommendation.py --config ecomm_prod.yml
    """
    if len(argv) == 2:
        return argv[1]
    if len(argv) == 3 and argv[1] == "--config":
        return argv[2]

    print("Usage: spark-submit consumption_recommendation.py <config_path>")
    print("   or: spark-submit consumption_recommendation.py --config <config_path>")
    sys.exit(1)


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
# Dimension & metrics builders
# -------------------------------------------------------
def build_item_dimension(seller_catalog_df):
    """
    Build item dimension from seller catalog (Gold Seller Catalog).

    - One row per item_id
    - Keep canonical item_name, category
    - Compute average catalog_price across sellers
    """
    item_dim = (
        seller_catalog_df
        .select("item_id", "item_name", "category", "marketplace_price")
        .filter(col("item_id").isNotNull())
    )

    item_dim = (
        item_dim
        .groupBy("item_id")
        .agg(
            first("item_name", ignorenulls=True).alias("item_name"),
            first("category", ignorenulls=True).alias("category"),
            spark_avg("marketplace_price").alias("catalog_price"),
        )
    )
    return item_dim


def build_company_metrics(company_sales_df):
    """
    Aggregate company sales by item_id from Company Sales Hudi table.
    """
    return (
        company_sales_df
        .groupBy("item_id")
        .agg(
            spark_sum("units_sold").alias("company_units_sold"),
            spark_sum("revenue").alias("company_revenue"),
        )
    )


def build_competitor_metrics(competitor_sales_df):
    """
    Aggregate competitor sales by item_id from Competitor Sales Hudi table.
    """
    return (
        competitor_sales_df
        .groupBy("item_id")
        .agg(
            spark_sum("units_sold").alias("competitor_units_sold"),
            spark_sum("revenue").alias("competitor_revenue"),
            spark_avg("marketplace_price").alias("competitor_avg_price"),
        )
    )


def build_seller_item_counts(seller_catalog_df):
    """
    Count in how many sellers' catalogs each item appears.
    """
    return (
        seller_catalog_df
        .groupBy("item_id")
        .agg(countDistinct("seller_id").alias("seller_count"))
    )


def build_item_metrics(item_dim, company_metrics, competitor_metrics, seller_counts):
    """
    Combine item dimension + company metrics + competitor metrics + seller counts.

    Steps:
    - Full outer join company + competitor metrics
    - Fill company/competitor totals with 0
    - Compute total_units_sold
    - Estimate company price (company_revenue / company_units_sold)
    - INNER JOIN with item_dim: keep only catalog-known items
    - LEFT JOIN with seller_counts, default seller_count to 1
    - Determine market_price preference:
        competitor_avg_price > company_price_estimate > catalog_price
    - Filter to items with total_units_sold > 0 and valid market_price
    - Compute expected_units_sold (per seller) and expected_revenue
    """
    metrics = company_metrics.join(competitor_metrics, on="item_id", how="full_outer")

    # Normalize nulls to 0 for numeric aggregations
    metrics = (
        metrics
        .withColumn("company_units_sold", coalesce(col("company_units_sold"), lit(0.0)))
        .withColumn("company_revenue", coalesce(col("company_revenue"), lit(0.0)))
        .withColumn("competitor_units_sold", coalesce(col("competitor_units_sold"), lit(0.0)))
        .withColumn("competitor_revenue", coalesce(col("competitor_revenue"), lit(0.0)))
    )

    # Total units sold across company + competitors
    metrics = metrics.withColumn(
        "total_units_sold",
        col("company_units_sold") + col("competitor_units_sold")
    )

    # Company price estimate (nullable if no company_units_sold)
    metrics = metrics.withColumn(
        "company_price_estimate",
        when(col("company_units_sold") > 0, col("company_revenue") / col("company_units_sold"))
    )

    # STRICT INNER JOIN — keep only items that exist in seller catalog
    metrics = metrics.join(item_dim, on="item_id", how="inner")

    # Seller counts (how many sellers currently carry each item)
    metrics = metrics.join(seller_counts, on="item_id", how="left")

    # Default seller_count to 1 if missing or non-positive
    metrics = metrics.withColumn(
        "seller_count",
        when(col("seller_count").isNull() | (col("seller_count") <= 0), lit(1))
        .otherwise(col("seller_count"))
    )

    # Market price resolution
    metrics = metrics.withColumn(
        "market_price",
        when(
            col("competitor_avg_price").isNotNull() & (col("competitor_avg_price") > 0),
            col("competitor_avg_price"),
        )
        .when(
            col("company_price_estimate").isNotNull() & (col("company_price_estimate") > 0),
            col("company_price_estimate"),
        )
        .otherwise(col("catalog_price"))
    )

    # Keep only items with demand + valid positive price
    metrics = metrics.filter(
        (col("total_units_sold") > 0) &
        col("market_price").isNotNull() &
        (col("market_price") > 0)
    )

    # Expected demand and revenue per seller if they list the item
    metrics = (
        metrics
        .withColumn("expected_units_sold", col("total_units_sold") / col("seller_count"))
        .withColumn("expected_revenue", col("expected_units_sold") * col("market_price"))
    )

    return metrics


# -------------------------------------------------------
# Top items (global)
# -------------------------------------------------------
def compute_top_items(item_metrics_df, top_n: int = 10):
    """
    Compute the global Top-N items by total_units_sold.

    The assignment requires:
    - Identify top 10 selling items overall (across company + marketplace)
    - Only for items with known item_name and category (from catalog)
    """
    catalog_items_only = item_metrics_df.filter(
        col("item_name").isNotNull() & col("category").isNotNull()
    )

    return (
        catalog_items_only
        .orderBy(col("total_units_sold").desc())
        .limit(top_n)
        .select(
            "item_id",
            "item_name",
            "category",
            "market_price",
            "expected_units_sold",
            "expected_revenue",
            "total_units_sold",
        )
    )


# -------------------------------------------------------
# Recommendations
# -------------------------------------------------------
def build_recommendations(seller_catalog_df, top_items_df):
    """
    For each seller, recommend top-N items they currently do not have.

    Logic:
    - Start from global Top-N items
    - Cross join with all sellers
    - Anti-join with existing seller-item pairs to keep only missing items
    """
    # All sellers
    sellers_df = seller_catalog_df.select("seller_id").distinct()

    # Existing seller-item relationships
    seller_items_df = seller_catalog_df.select("seller_id", "item_id").distinct()

    # Candidate pairs: every seller x every top item
    candidate_pairs = sellers_df.crossJoin(top_items_df.select("item_id"))

    # Keep only (seller, item) where seller does NOT already have that item
    missing_pairs = candidate_pairs.join(
        seller_items_df,
        on=["seller_id", "item_id"],
        how="left_anti",
    )

    # Attach item details and expected metrics
    recommendations_df = missing_pairs.join(top_items_df, on="item_id", how="inner")

    recommendations_df = recommendations_df.select(
        "seller_id",
        "item_id",
        "item_name",
        "category",
        "market_price",
        "expected_units_sold",
        "expected_revenue",
    )

    return recommendations_df


# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
def main():
    """
    Recommendation consumption pipeline.

    Inputs (Gold Hudi tables):
    - Seller Catalog (validated seller-item metadata)
    - Company Sales
    - Competitor Sales

    Steps:
    - Read Hudi Gold tables
    - Build item dimension and metrics across company + competitors
    - Compute global top 10 items by total_units_sold
    - For each seller, recommend top 10 items they do not currently list
    - Write recommendations as a single CSV (coalesced) to output_path
    """
    try:
        config_path = get_config_path(sys.argv)
        config = load_config(config_path)

        rec_conf = config["recommendation"]
        seller_catalog_path = rec_conf["seller_catalog_hudi"]
        company_sales_path = rec_conf["company_sales_hudi"]
        competitor_sales_path = rec_conf["competitor_sales_hudi"]
        output_path = rec_conf["output_csv"]

        spark = get_spark_session("Seller Recommendation Consumption")

        print(f"[INFO] Reading Seller Catalog Hudi: {seller_catalog_path}")
        seller_catalog_df = spark.read.format("hudi").load(seller_catalog_path)

        print(f"[INFO] Reading Company Sales Hudi:  {company_sales_path}")
        company_sales_df = spark.read.format("hudi").load(company_sales_path)

        print(f"[INFO] Reading Competitor Sales Hudi: {competitor_sales_path}")
        competitor_sales_df = spark.read.format("hudi").load(competitor_sales_path)

        # Build dimensions and metrics
        print("[INFO] Building item dimension and metrics")
        item_dim = build_item_dimension(seller_catalog_df)
        company_metrics = build_company_metrics(company_sales_df)
        competitor_metrics = build_competitor_metrics(competitor_sales_df)
        seller_counts = build_seller_item_counts(seller_catalog_df)

        item_metrics_df = build_item_metrics(
            item_dim=item_dim,
            company_metrics=company_metrics,
            competitor_metrics=competitor_metrics,
            seller_counts=seller_counts,
        )

        # Compute global Top-N items
        print("[INFO] Computing global Top-10 items")
        top_items_df = compute_top_items(item_metrics_df, top_n=10)

        # Build per-seller recommendations
        print("[INFO] Building per-seller recommendations")
        recommendations_df = build_recommendations(seller_catalog_df, top_items_df)

        # Write out a single CSV file (coalesced)
        print(f"[INFO] Writing recommendations CSV to: {output_path}")
        (
            recommendations_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(output_path)
        )

        spark.stop()
        print("[SUCCESS] Recommendation pipeline completed successfully")

    except Exception as e:
        print(f"[ERROR] Recommendation pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
