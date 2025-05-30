# Build a Delta Live Tables Pipeline with Medallion Architecture

## Prerequisites

- Access to a Databricks workspace with Delta Live Tables enabled.
- Upload your sample data CSV file (`sales.csv`) to Databricks FileStore or cloud storage accessible by Databricks.

---

## Step 1: Upload Sample Data

1. In Databricks, go to **Data > DBFS**.
2. Click **Upload** and select `sales.csv`.
3. Upload to: `/FileStore/sales.csv`.

*Note: You can also upload via CLI or directly to cloud storage (Azure Blob, S3) and update paths accordingly.*

---

## Step 2: Create Notebooks for DLT Pipeline

### Notebook 1: Bronze and Silver Layers (Python)

Create a new Python notebook named `Medallion_Pipeline_Python_Bronze_Silver` and add:

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="bronze_sales",
  comment="Raw sales data ingested to Bronze layer",
  table_properties={
    "quality": "bronze"
  }
)
def bronze_sales():
    return (
        spark.read.option("header", "true")
                  .option("inferSchema", "true")
                  .csv("/FileStore/sales.csv")
    )

@dlt.table(
  name="silver_sales",
  comment="Cleaned and deduplicated Silver sales data"
)
@dlt.expect("valid_quantity", "Quantity > 0")
@dlt.expect_or_drop("no_nulls", "CustomerId IS NOT NULL")
def silver_sales():
    df = dlt.read("bronze_sales")
    return df.filter(col("Quantity") > 0).dropDuplicates(["SalesOrderNumber", "SalesOrderLineNumber"])

```

---

### Notebook 2: Gold Layer (SQL)

Create a new SQL notebook named `Medallion_Pipeline_SQL_Gold` and add:

```sql
CREATE LIVE TABLE gold_sales_summary
COMMENT "Aggregated daily sales totals"
AS
SELECT
  OrderDate,
  SUM(Quantity * UnitPrice) AS total_sales,
  COUNT(DISTINCT CustomerId) AS unique_customers
FROM
  LIVE.silver_sales
GROUP BY
  OrderDate;

ALTER LIVE TABLE gold_sales_summary
ADD EXPECTATION valid_total_sales AS total_sales > 0;

```

---

## Step 3: Create Delta Live Tables Pipeline

1. Go to **Jobs > Delta Live Tables** in Databricks.
2. Click **Create Pipeline**.
3. Configure:
     - **Name:** `MedallionArchitecturePipeline`
     - **Pipeline Mode:** Triggered (manual) or Continuous (streaming)
     - **Cluster:** 8+ GB memory, 2+ cores recommended
     - **Notebook Libraries:** Attach both notebooks above
     - **Storage Location:** e.g., `/pipelines/medallion_architecture`
     - **Target Schema:** Default or specify a database
4. Click **Create**.

---

## Step 4: Run the Pipeline

- In the DLT pipeline UI, click **Start** or **Trigger run**.
- Monitor progress:
    - Tables: `bronze_sales`, `silver_sales`, `gold_sales_summary`
    - Data quality expectations
    - Logs for errors

---

## Step 5: Query and Verify Data

Run in Databricks SQL notebook or editor:

```sql
SELECT * FROM uc01.sales.bronze_sales LIMIT 10;
SELECT * FROM uc01.sales.silver_sales LIMIT 10;
SELECT * FROM uc01.sales.gold_sales_summary LIMIT 10;
```

- Confirm raw data in Bronze, cleaned data in Silver, and aggregated metrics in Gold.

---

## Step 6: Monitor & Manage Pipeline

- Use the Delta Live Tables UI to:
    - Check pipeline health and status
    - Review expectation pass/fail counts
    - Explore execution logs
    - Restart or stop pipeline as needed

---

## Best Practices

- Use expectations to ensure data quality.
- Build and test each layer incrementally.
- Modularize complex transformations.
- Schedule runs based on data arrival.
- Monitor pipelines regularly.
