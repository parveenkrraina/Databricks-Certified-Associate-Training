# Building SQL Delta Live Tables Pipelines with Expectations

**Duration:** ~60 minutes  
**Prerequisites:**  
- Access to Databricks workspace with Delta Live Tables enabled  
- Sample dataset uploaded (e.g., raw sales data CSV)

---

## Lab Objectives

- Create a Delta Live Tables pipeline using SQL declarative syntax
- Define Bronze, Silver, and Gold tables implementing the Medallion architecture
- Configure and apply data quality expectations in SQL pipelines
- Run and monitor the pipeline in the Delta Live Tables UI
- Observe pipeline logs and alerts triggered by expectation failures

---

## Step 1: Prepare Sample Data

- Upload or ensure sample raw data is available in a storage location accessible by Databricks (e.g., DBFS or cloud storage).
- **Example dataset:** `sales_raw.csv` with columns: `order_id`, `customer_id`, `order_date`, `item`, `quantity`, `unit_price`, `tax_amount`.

---

## Step 2: Create a New SQL Pipeline in Delta Live Tables UI

1. Go to the Delta Live Tables section in your Databricks workspace.
2. Click **Create Pipeline**.
3. Enter pipeline name, e.g., `medallion_sql_pipeline`.
4. Select appropriate cluster configuration (default is fine).
5. Set pipeline mode to **Batch**.
6. Specify a storage location for the pipeline (DBFS path or cloud storage).
7. Under Pipeline settings, select **SQL** as the language.
8. Save the pipeline (don’t start it yet).

---

## Step 3: Write SQL Pipeline Script

Create the pipeline script defining the Bronze, Silver, and Gold tables with expectations.

```sql
-- Bronze layer: raw ingestion (ingest all raw data as-is)
-- Load the CSV file into a Delta table with correct column names
CREATE LIVE TABLE bronze_sales_raw_delta AS
SELECT
  _c0 AS SalesOrderNumber,
  _c1 AS SalesOrderLineNumber,
  CAST(_c2 AS DATE) AS OrderDate,
  _c3 AS CustomerId,
  _c4 AS CustomerName,
  _c5 AS EmailAddress,
  _c6 AS Item,
  _c7 AS Quantity,
  _c8 AS UnitPrice,
  _c9 AS TaxAmount
FROM csv.`/FileStore/sales.csv`;

-- Create the live table from the Delta table
CREATE LIVE TABLE bronze_sales_raw AS
SELECT *
FROM bronze_sales_raw_delta;

-- Silver layer: clean and deduplicate, add data quality expectations
CREATE LIVE TABLE silver_sales_cleaned (
  SalesOrderNumber STRING NOT NULL,
  SalesOrderLineNumber STRING,
  OrderDate DATE NOT NULL,
  CustomerId STRING,
  CustomerName STRING,
  EmailAddress STRING,
  Item STRING,
  Quantity INT,
  UnitPrice DOUBLE,
  TaxAmount DOUBLE,
  CONSTRAINT order_id_not_null EXPECT (SalesOrderNumber IS NOT NULL),
  CONSTRAINT order_date_not_null EXPECT (OrderDate IS NOT NULL),
  CONSTRAINT email_format EXPECT (EmailAddress LIKE '%@%'),
  CONSTRAINT quantity_valid EXPECT (Quantity BETWEEN 1 AND 100),
  CONSTRAINT price_positive EXPECT (UnitPrice > 0),
  CONSTRAINT tax_non_negative EXPECT (TaxAmount >= 0),
  CONSTRAINT order_date_valid EXPECT (OrderDate <= CURRENT_DATE())
)
AS
SELECT
  SalesOrderNumber,
  SalesOrderLineNumber,
  OrderDate,
  CustomerId,
  CustomerName,
  EmailAddress,
  Item,
  CAST(Quantity AS INT) AS Quantity,
  CAST(UnitPrice AS DOUBLE) AS UnitPrice,
  CAST(TaxAmount AS DOUBLE) AS TaxAmount
FROM live.bronze_sales_raw
WHERE SalesOrderNumber IS NOT NULL
  AND OrderDate IS NOT NULL;


-- Gold layer: aggregated sales data for reporting
CREATE LIVE TABLE gold_sales_aggregated AS
SELECT
  CustomerId,
  DATE_TRUNC('month', OrderDate) AS SalesMonth,
  COUNT(DISTINCT SalesOrderNumber) AS NumberOfOrders,
  SUM(Quantity) AS TotalQuantity,
  SUM(Quantity * UnitPrice) AS TotalSales,
  SUM(TaxAmount) AS TotalTax
FROM live.silver_sales_cleaned
GROUP BY CustomerId, DATE_TRUNC('month', OrderDate);
```

- Paste this SQL script into the pipeline editor in the UI.
- Save the script.

---

## Step 4: Start and Run the Pipeline

- Click **Start** to run the pipeline.
- Monitor pipeline progress in the UI dashboard.
- Wait until the pipeline run completes successfully.

---

## Step 5: Review Pipeline Results and Quality Checks

- Navigate to the **Expectations** tab of the pipeline run.
- Confirm that all expectations passed.
- Intentionally modify the raw data (e.g., introduce duplicate or null `order_id`) and rerun the pipeline.
- Observe how failing expectations trigger alerts or pipeline errors.
- Check logs to understand the cause of failure.

---

## Step 6: Query Result Tables

In a Databricks notebook or SQL editor, run queries to validate output:

```sql
-- Bronze raw data
SELECT * FROM uc01.sales.bronze_sales_raw LIMIT 10;

-- Silver cleaned data
SELECT * FROM uc01.sales.silver_sales_cleaned LIMIT 10;

-- Gold aggregated data
SELECT * FROM uc01.sales.gold_sales_aggregated ORDER BY order_date DESC LIMIT 10;
```