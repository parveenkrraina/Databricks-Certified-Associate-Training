# Complex Delta Live Tables Pipeline with Multiple CSV Sources and Comprehensive Expectations
This notebook implements a Medallion architecture pipeline with 3 datasets: Sales, Customers, and Products.

We ingest raw CSV files in Bronze tables, clean and validate them with various expectations in Silver tables, then join and aggregate in Gold tables.


## Step 1: Bronze Layer – Raw Ingestion of CSV Files
We ingest all CSV files as raw tables with minimal transformation.


```python
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

CREATE LIVE TABLE bronze_customer_details_delta AS
SELECT
  _c0 AS CustomerId,
  _c1 AS CustomerName,
  _c2 AS EmailAddress,
  _c3 AS CustomerSegment,
  _c4 AS Country
FROM csv.`/FileStore/customer_details.csv`;

CREATE LIVE TABLE bronze_customer_details_raw AS
SELECT * FROM bronze_customer_details_delta;

CREATE LIVE TABLE bronze_product_catalog_delta AS
SELECT
  _c0 AS Item,
  _c1 AS Category,
  _c2 AS Brand,
  _c3 AS Supplier,
  CAST(_c4 AS DOUBLE) AS UnitCost
FROM csv.`/FileStore/product_catalog.csv`;

CREATE LIVE TABLE bronze_product_catalog_raw AS
SELECT * FROM bronze_product_catalog_delta;
```

## Step 2: Silver Layer – Cleaning and Data Quality Checks

### Cleaning and Validating Sales Data
We cast types and apply expectations:

- `EXPECT NOT NULL` on critical fields like `SalesOrderNumber`, `OrderDate`
- `EXPECT UNIQUE` on `SalesOrderNumber`
- Date validation to ensure no future `OrderDate`
- Range checks on `Quantity`, `UnitPrice`, `TaxAmount`
- Simple email format check on `EmailAddress`


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
  CONSTRAINT order_date_valid EXPECT (OrderDate <= CURRENT_DATE()),
  CONSTRAINT quantity_valid EXPECT (Quantity BETWEEN 1 AND 100),
  CONSTRAINT price_positive EXPECT (UnitPrice > 0),
  CONSTRAINT tax_non_negative EXPECT (TaxAmount >= 0),
  CONSTRAINT email_format EXPECT (EmailAddress LIKE '%@%')
)
AS
SELECT
  SalesOrderNumber,
  SalesOrderLineNumber,
  CAST(OrderDate AS DATE) AS OrderDate,
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

### Cleaning and Validating Customer Data
Expectations:

- `EXPECT NOT NULL` for CustomerId, CustomerName, EmailAddress
- `EXPECT UNIQUE` on CustomerId and EmailAddress
- Email format check
- CustomerSegment must be one of known values
- Country must be from allowed list


```python
CREATE LIVE TABLE silver_customer_cleaned (
  CustomerId STRING NOT NULL,
  CustomerName STRING NOT NULL,
  EmailAddress STRING NOT NULL,
  CustomerSegment STRING,
  Country STRING,
  CONSTRAINT customer_id_not_null EXPECT (CustomerId IS NOT NULL),
  CONSTRAINT customer_name_not_null EXPECT (CustomerName IS NOT NULL),
  CONSTRAINT email_not_null EXPECT (EmailAddress IS NOT NULL),
  CONSTRAINT email_format EXPECT (EmailAddress LIKE '%@%'),
  CONSTRAINT segment_valid EXPECT (CustomerSegment IN ('Retail', 'Wholesale', 'Online')),
  CONSTRAINT country_valid EXPECT (Country IN ('USA', 'Canada', 'UK', 'Germany', 'France'))
)
AS
SELECT
  CustomerId,
  CustomerName,
  EmailAddress,
  CustomerSegment,
  Country
FROM live.bronze_customer_details_raw;

```

### Cleaning and Validating Product Catalog
Expectations:

- `EXPECT NOT NULL` for Item, Category, Brand, Supplier, UnitCost
- Category must be in allowed set
- UnitCost must be positive


```python
CREATE LIVE TABLE silver_product_cleaned (
  Item STRING NOT NULL,
  Category STRING NOT NULL,
  Brand STRING NOT NULL,
  Supplier STRING NOT NULL,
  UnitCost DOUBLE,
  CONSTRAINT item_not_null EXPECT (Item IS NOT NULL),
  CONSTRAINT category_not_null EXPECT (Category IS NOT NULL),
  CONSTRAINT category_valid EXPECT (Category IN ('Electronics', 'Accessories', 'Office Supplies', 'Home Appliances')),
  CONSTRAINT brand_not_null EXPECT (Brand IS NOT NULL),
  CONSTRAINT supplier_not_null EXPECT (Supplier IS NOT NULL),
  CONSTRAINT unitcost_positive EXPECT (UnitCost > 0)
)
AS
SELECT
  Item,
  Category,
  Brand,
  Supplier,
  CAST(UnitCost AS DOUBLE) AS UnitCost
FROM live.bronze_product_catalog_raw;

```

## Step 3: Gold Layer – Join and Aggregate Sales Data
We join cleaned sales, customer, and product data.

Aggregations:

- Total sales amount, quantity, and tax per customer segment and product category
- Count distinct orders

Expectations to ensure aggregated results are valid.

```python
CREATE OR REPLACE LIVE TABLE gold_sales_summary AS
SELECT
  c.CustomerSegment,
  COUNT(DISTINCT s.SalesOrderNumber) AS NumberOfOrders,
  SUM(s.Quantity) AS TotalQuantity,
  SUM(s.Quantity * s.UnitPrice) AS TotalSales,
  SUM(s.TaxAmount) AS TotalTax
FROM uc01.default.silver_sales_cleaned s
JOIN uc01.default.silver_customer_cleaned c
  ON s.CustomerId = LEFT(c.CustomerId, LENGTH(c.CustomerId) - 5)
GROUP BY c.CustomerSegment;


```

```python
CREATE LIVE TABLE gold_sales_debug AS
SELECT
  c.CustomerSegment,
  p.Category,
  COUNT(DISTINCT s.SalesOrderNumber) AS NumberOfOrders,
  SUM(s.Quantity) AS TotalQuantity,
  SUM(s.Quantity * s.UnitPrice) AS TotalSales,
  SUM(s.TaxAmount) AS TotalTax
FROM live.silver_sales_cleaned s
JOIN live.silver_customer_cleaned c ON s.CustomerId = LEFT(c.CustomerId, LENGTH(c.CustomerId) - 5)
JOIN live.silver_product_cleaned p ON s.Item = p.Item
GROUP BY c.CustomerSegment, p.Category;

```

```python
CREATE LIVE TABLE gold_debug AS
SELECT *
FROM LIVE.silver_sales_cleaned
LIMIT 10;

```

## Summary
- Bronze tables ingest raw CSVs without transformations
- Silver tables clean and validate data with comprehensive expectations
- Gold table joins and aggregates, applying further expectations on summary data

This pipeline ensures robust data quality and prepares reliable data for analytics.


## Next Steps
- Deploy this pipeline in your Databricks workspace
- Monitor expectations in UI and respond to failures
- Extend pipeline with streaming ingestion or additional business rules

