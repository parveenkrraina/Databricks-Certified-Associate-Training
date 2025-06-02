# Delta Live Tables SQL Pipeline with Complex Data Quality Expectations
This notebook implements a Medallion Architecture pipeline (Bronze, Silver, Gold) using Delta Live Tables SQL syntax.

We will ingest raw sales data, clean and validate it with multiple data quality expectations, then aggregate it for reporting.

## Step 1: Bronze Layer - Raw Data Ingestion

In this step, we ingest the raw CSV sales data as-is, casting minimal types.

This layer acts as the immutable source data in our pipeline.

```python
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
FROM LIVE.bronze_sales_raw_delta;
```

## Step 2: Silver Layer - Data Cleaning and Quality Checks

This layer cleans the raw data by applying type casting and basic filtering.

We also apply multiple **data quality expectations** to ensure the data integrity before it flows downstream.

**Explanation of Expectations:**

- `EXPECT NOT NULL(SalesOrderNumber)`  
  Ensures every row has a sales order number. Null values here would invalidate the record.

- `EXPECT UNIQUE(SalesOrderNumber)`  
  Guarantees uniqueness of sales orders, avoiding duplicates.

- `EXPECT NOT NULL(OrderDate)`  
  Order date is mandatory to analyze sales over time.

- `EXPECT(OrderDate <= CURRENT_DATE())`  
  Prevents future dates which are invalid in historical data.

- `EXPECT(Quantity BETWEEN 1 AND 100)`  
  Ensures quantity is within reasonable bounds.

- `EXPECT(UnitPrice > 0)`  
  Unit price must be positive, no free or negative prices allowed.

- `EXPECT(TaxAmount >= 0)`  
  Tax amount cannot be negative.

- `EXPECT EmailAddress LIKE '%@%'`  
  Basic check to ensure email address has an '@' symbol (simple email format validation).

These expectations act as guardrails. If any fail during pipeline runs, alerts or failures are triggered, helping maintain clean and reliable data.

The **constraint clause** is a SQL conditional statement that must evaluate to true or false for each record. The constraint contains the actual logic for what is being validated. When a record fails this condition, the expectation is triggered.

Constraints must use valid SQL syntax and cannot contain the following:

- Custom Python functions
- External service calls
- Subqueries referencing other tables

```sql
-- Simple constraint
CONSTRAINT non_negative_price EXPECT (price >= 0)

-- SQL functions
CONSTRAINT valid_date EXPECT (year(transaction_date) >= 2020)

-- CASE statements
CONSTRAINT valid_order_status EXPECT (
    CASE
        WHEN type = 'ORDER' THEN status IN ('PENDING', 'COMPLETED', 'CANCELLED')
        WHEN type = 'REFUND' THEN status IN ('PENDING', 'APPROVED', 'REJECTED')
        ELSE false
    END
)

-- Multiple constraints
CONSTRAINT non_negative_price EXPECT (price >= 0)
CONSTRAINT valid_purchase_date EXPECT (date <= current_date())

-- Complex business logic
CONSTRAINT valid_subscription_dates EXPECT (
    start_date <= end_date
    AND end_date <= current_date()
    AND start_date >= '2020-01-01'
)

-- Complex boolean logic
CONSTRAINT valid_order_state EXPECT (
    (status = 'ACTIVE' AND balance > 0)
    OR (status = 'PENDING' AND created_date > current_date() - INTERVAL 7 DAYS)
)
```

```python
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
```

## Step 3: Gold Layer - Aggregations for Reporting

In this final layer, we aggregate sales metrics monthly by customer.

Aggregations provide meaningful insights like total quantity sold, total sales amount, and total tax per customer per month.

This table is typically consumed by business intelligence or analytics tools.

```python
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

## Summary

- The **Bronze** table ingests raw data directly from CSV.  
- The **Silver** table cleans and validates data, applying multiple expectations to ensure quality.  
- The **Gold** table aggregates sales data, ready for downstream consumption.  

This pipeline showcases the power of Delta Live Tables declarative SQL pipelines combined with robust data quality enforcement through expectations.

### Next Steps

- Run this pipeline in your Delta Live Tables environment.  
- Monitor expectation results in the UI; experiment by modifying input data to trigger failures.  
- Extend the pipeline with streaming ingestion or additional quality checks.
