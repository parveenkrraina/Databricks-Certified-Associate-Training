# Session 1 Hands-On Lab Guide
## Data Ingestion with Delta Lake

### Lab Objectives
- Load a large sales dataset with common data issues into Delta Lake
- Clean data by handling missing values, duplicates, and type corrections
- Perform basic DataFrame transformations
- Query data using Spark SQL
- Understand practical Delta Lake ingestion and transformation workflows


### Prerequisites
- Access to Databricks or Azure Synapse Spark pool
- Sample data CSV file uploaded to workspace/storage (`sales.csv`)
- Basic familiarity with PySpark and SQL


## Step 1: Load CSV into Spark DataFrame

```python
# Load CSV with inferSchema
sales_df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("dbfs:/FileStore/shared_uploads/parveen.r@live.com/sales.csv")

print("Initial data sample:")
sales_df.show(5)
```


## Step 2: Inspect Data and Identify Issues

- Notice missing `EmailAddress` and `Quantity`
- Duplicate rows present
- Some columns have inconsistent data types (`Quantity`, `UnitPrice`, `TaxAmount`)
- `ProductMetadata` contains JSON strings


## Step 3: Data Cleaning

- Remove duplicate rows
- Fill or drop missing values in critical columns
- Convert `Quantity` and `UnitPrice` to correct numeric types
- Cast `TaxAmount` to float, handling string types

```python
from pyspark.sql.functions import col, when

# Remove duplicates
sales_df_clean = sales_df.dropDuplicates()

# Drop rows with missing Quantity or CustomerName (critical fields)
sales_df_clean = sales_df_clean.dropna(subset=["Quantity", "CustomerName"])

# Fix data types - convert Quantity to integer safely
sales_df_clean = sales_df_clean.withColumn(
    "Quantity",
    when(col("Quantity").cast("int").isNotNull(), col("Quantity").cast("int")).otherwise(None)
)

# Convert UnitPrice to float, set invalid to null
sales_df_clean = sales_df_clean.withColumn(
    "UnitPrice",
    when(col("UnitPrice").cast("float").isNotNull(), col("UnitPrice").cast("float")).otherwise(None)
)

# Convert TaxAmount to float
sales_df_clean = sales_df_clean.withColumn(
    "TaxAmount",
    when(col("TaxAmount").cast("float").isNotNull(), col("TaxAmount").cast("float")).otherwise(None)
)

# Drop rows where conversions failed (null in Quantity or UnitPrice)
sales_df_clean = sales_df_clean.dropna(subset=["Quantity", "UnitPrice"])

print("Cleaned data sample:")
sales_df_clean.show(5)
```


## Step 4: Write Cleaned Data as Delta Table

```python
# Write cleaned data as Delta table
sales_df_clean.write.format("delta").mode("overwrite").saveAsTable("sales_delta_cleaned")

print("Cleaned Delta table created: sales_delta_cleaned")
```


## Step 5: Basic DataFrame Transformations

```python
from pyspark.sql.functions import col

# Load cleaned Delta table
df = spark.table("sales_delta_cleaned")

# Add TotalPrice column
df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

# Filter rows where Quantity > 5
filtered_df = df.filter(col("Quantity") > 5)

filtered_df.select(
    "SalesOrderNumber", "CustomerName", "Item", "Quantity", "UnitPrice", "TotalPrice"
).show(10)
```


## Step 6: Query Delta Table Using SQL

```sql
-- Total sales per customer
SELECT CustomerName, SUM(Quantity) AS TotalQuantity, SUM(Quantity * UnitPrice) AS TotalSales
FROM sales_delta_cleaned
GROUP BY CustomerName
ORDER BY TotalSales DESC
LIMIT 10;
```


## Optional Step 7: Parse JSON Column (`ProductMetadata`)

```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("color", StringType(), True),
    StructField("warranty", StringType(), True)
])

df_with_json = df.withColumn("ProductDetails", from_json(col("ProductMetadata"), schema))

df_with_json.select(
    "SalesOrderNumber", "ProductMetadata", "ProductDetails.color", "ProductDetails.warranty"
).show(5)
```
