#!/bin/bash
set -e

echo "============================================================"
echo "Running ETL 3 - Competitor Sales"
echo "============================================================"

PROJECT_ROOT="/home/sabya/2025EM1100189/ecommerce_seller_recommendation/local"
SPARK_BIN="/opt/spark/bin/spark-submit"
HUDI_JAR="/opt/spark/jars/hudi-spark3.5-bundle_2.12-0.15.0.jar"

CONFIG="$PROJECT_ROOT/configs/ecomm_prod.yml"
SCRIPT="$PROJECT_ROOT/src/etl_competitor_sales.py"

echo "Config : $CONFIG"
echo "Script : $SCRIPT"

$SPARK_BIN \
  --master local[*] \
  --jars "$HUDI_JAR" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.legacy.timeParserPolicy=LEGACY" \
  "$SCRIPT" \
  --config "$CONFIG"

echo "============================================================"
echo "ETL 3 Completed Successfully!"
echo "============================================================"
