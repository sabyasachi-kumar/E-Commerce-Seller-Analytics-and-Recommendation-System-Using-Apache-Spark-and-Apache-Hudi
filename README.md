# E-commerce Top-Seller Items Recommendation System

## 1. Introduction
This project implements a complete data engineering pipeline for processing marketplace data and generating seller-specific recommendations of top-selling items. The system ingests raw data into a structured medallion architecture (Bronze → Silver → Gold), enforces rigorous data quality rules, stores cleaned datasets as Hudi tables, and produces actionable insights through a recommendation engine that analyzes market demand across the company and competitors.

**Key Technologies:** Apache Spark (PySpark), Apache Hudi 0.15.0, WSL2 Ubuntu

---

## 2. Project Structure

```
ecommerce_seller_recommendation/
│
├── configs/
│   └── ecomm_prod.yml                # Pipeline configuration
│
├── raw/                              # Bronze layer: Raw CSV inputs
│   ├── seller_catalog/
│   ├── company_sales/
│   └── competitor_sales/
│
├── processed/                        # Gold layer: Validated Hudi tables
│   ├── seller_catalog_hudi/
│   ├── company_sales_hudi/
│   ├── competitor_sales_hudi/
│   └── recommendations_csv/          # Final recommendation output
│
├── quarantine/                       # Data quality failures
│   ├── seller_catalog/
│   ├── company_sales/
│   └── competitor_sales/
│
├── src/
│   ├── etl_seller_catalog.py
│   ├── etl_company_sales.py
│   ├── etl_competitor_sales.py
│   └── consumption_recommendation.py
│
├── scripts/                          # Spark submit wrappers
│   ├── etl_seller_catalog_spark_submit.sh
│   ├── etl_company_sales_spark_submit.sh
│   ├── etl_competitor_sales_spark_submit.sh
│   └── consumption_recommendation_spark_submit.sh
│
└── README.md
```

---

## 3. System Architecture

### 3.1 Medallion Architecture

**Bronze Layer – Raw Ingestion**

* Direct CSV ingestion without transformations
* Immutable source of truth

**Silver Layer – Standardization and Cleaning**

* Trimming, type casting, and normalization
* Date parsing with legacy policy
* Deduplication (dataset-specific)
* Assignment of data quality flags

**Gold Layer – Validated Storage**

* Only DQ-passed records
* Stored as Apache Hudi Copy-On-Write tables
* Schema evolution enabled
* Optimized for analytical consumption

**Quarantine Layer – Failed Records**

* Invalid rows tagged with reasons
* Enables reprocessing and auditing

**Consumption Layer – Recommendation Engine**

* Combines Hudi tables
* Computes market intelligence
* Generates seller-specific top-item recommendations

### 3.2 Technology Stack

* Apache Spark 3.x (PySpark)
* Apache Hudi 0.15.0
* Kryo serialization
* WSL2 Ubuntu runtime
* YAML-based configuration

---

## 4. ETL Pipeline Specifications

### 4.1 Seller Catalog ETL (`etl_seller_catalog.py`)

**Silver Transformations**

* Trim all string fields
* Title-case `item_name` and `category`
* Cast `marketplace_price` → DOUBLE, `stock_qty` → INT
* Fill missing `stock_qty` with 0
* Deduplicate on `(seller_id, item_id)`

**Data Quality Rules**

* `seller_id`, `item_id`, `item_name`, `category` must not be null
* Prices and stock quantities must be non-negative

**Gold Output**

* Hudi table: `processed/seller_catalog_hudi/`
* Record key: `seller_id,item_id`
* Precombine field: `ingestion_timestamp`

---

### 4.2 Company Sales ETL (`etl_company_sales.py`)

**Silver Transformations**

* Trim `item_id`
* Type cast: `units_sold` INT, `revenue` DOUBLE, `sale_date` DATE
* Fill null numeric values with 0
* Deduplicate on `item_id` (aggregated dataset)

**Data Quality Rules**

* `item_id` must not be null
* Numeric fields must be non-negative
* `sale_date` must be valid and ≤ current date

**Gold Output**

* Hudi table: `processed/company_sales_hudi/`
* Record key: `item_id`
* Precombine: `ingestion_timestamp`

---

### 4.3 Competitor Sales ETL (`etl_competitor_sales.py`)

**Silver Transformations**

* Trim seller and item identifiers
* Type cast numeric and date fields
* Fill null numeric values with 0
* Preserve all rows (time-series data; no deduplication)

**Data Quality Rules**

* `seller_id`, `item_id` must not be null
* Prices and sales metrics must be non-negative
* `sale_date` must be valid

**Gold Output**

* Hudi table: `processed/competitor_sales_hudi/`
* Record key: `seller_id,item_id`
* Precombine: `sale_date`

---

## 5. Recommendation Engine (`consumption_recommendation.py`)


### 5.1 Data Inputs

The recommendation script loads the following **Gold** Hudi tables:

* `seller_catalog_hudi`
* `company_sales_hudi`
* `competitor_sales_hudi`

These tables already contain validated and clean data, so no further DQ checks are required in the consumption layer.

---

## 5.2 Item Dimension Construction

Derived from the validated seller catalog:

* One row per `item_id`
* Canonical `item_name` and `category` selected using `first()`
* `catalog_price` = average marketplace price across sellers
* Null item_ids filtered out

---

## 5.3 Metrics Aggregation

### **Company Metrics**

Aggregated by `item_id`:

* `company_units_sold`
* `company_revenue`

### **Competitor Metrics**

Aggregated by `item_id`:

* `competitor_units_sold`
* `competitor_revenue`
* `competitor_avg_price`

### **Seller Item Counts**

* Number of distinct sellers currently offering each item
* Defaulted to 1 for missing or zero counts

---

## 5.4 Unified Item Metrics Computation

The script merges all metrics into a unified item analytics table:

### Key Steps

1. **Full outer join** of company and competitor metrics

2. Replace null numeric fields with 0

3. Compute `total_units_sold` = company + competitor

4. Estimate company price:

   ```
   company_price_estimate = company_revenue / company_units_sold
   ```

5. **Inner join** with item dimension (only items known in catalog retained)

6. Join with seller counts (default = 1 where missing)

7. **Market price resolution hierarchy:**

   1. Competitor average price (if > 0)
   2. Company estimated price (if > 0)
   3. Catalog price

8. **Filtering criteria**

   * `total_units_sold` > 0
   * `market_price` > 0

9. Compute expected future performance:

   ```
   expected_units_sold = total_units_sold / seller_count
   expected_revenue = expected_units_sold * market_price
   ```

---

## 5.5 Top-N Item Extraction

The top items are selected by:

* Sorting `total_units_sold` in descending order
* Considering only items with valid name and category
* Selecting the **top 10** items

Each selected item includes:

* Item identifiers and metadata
* Market price
* Expected units and expected revenue
* Total units sold

---

## 5.6 Per-Seller Recommendation Logic

To generate personalized recommendations:

1. Extract all distinct sellers
2. Generate candidate combinations:

   ```
   seller × top_10_items  (CROSS JOIN)
   ```
3. Exclude items the seller already offers using **left anti join**
4. Attach item-level metrics and metadata
5. Select final fields:

   * `seller_id`
   * `item_id`
   * `item_name`
   * `category`
   * `market_price`
   * `expected_units_sold`
   * `expected_revenue`

---

## 5.7 Output

* Output is written as **one consolidated CSV file** using `coalesce(1)`
* Stored at `processed/recommendations_csv/`
* Overwrites existing directory
* Includes header

---

## 6. Configuration Management

The YAML file `configs/ecomm_prod.yml` defines:

* Raw input paths
* Gold output paths
* Quarantine paths
* Recommendation output path

The pipeline is fully portable across environments.

---

## 7. Execution Instructions

Run all scripts from the project root:

### Step 1 – Seller Catalog ETL

```bash
./scripts/etl_seller_catalog_spark_submit.sh
```

### Step 2 – Company Sales ETL

```bash
./scripts/etl_company_sales_spark_submit.sh
```

### Step 3 – Competitor Sales ETL

```bash
./scripts/etl_competitor_sales_spark_submit.sh
```

### Step 4 – Recommendation Engine

```bash
./scripts/consumption_recommendation_spark_submit.sh
```

---

## 8. Data Quality Assurance

* Mandatory fields validated in Silver
* Business constraints enforced
* Invalid rows isolated with detailed failure reasons
* Gold layer contains only guaranteed-valid records
* Consumption layer assumes validated inputs

---

## 9. Design Decisions

* **Copy-On-Write Hudi tables** chosen for analytical workloads
* **Legacy date parser** improves compatibility with raw inputs
* **Seller count defaulting to 1** avoids division errors
* **Competitor price prioritized** for realistic market price estimation
* **Strict inner join with item dimension** ensures consistency

---

## 10. Testing and Validation

The following aspects were validated:

* Directory structure matches specification
* Paths in YAML reflect actual file locations
* ETL transformations align with assignment
* All data quality rules enforce correctness
* Recommendation logic correctly implements assignment requirements
* Hudi tables load and process successfully in WSL2
* Final CSV is generated in expected structure

---

## 11. Conclusion

This project delivers a robust, scalable, and maintainable data engineering solution for e-commerce analytics. It applies a structured medallion architecture, enforces strict data quality, integrates company and competitor market intelligence, and produces precise seller-level recommendations based on demand, pricing, and expected performance.

---

## 12. References

* Apache Hudi Documentation – [https://hudi.apache.org/](https://hudi.apache.org/)
* Apache Spark Documentation – [https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/)
