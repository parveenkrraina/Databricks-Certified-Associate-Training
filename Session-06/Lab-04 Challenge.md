# Part 7: Challenge Lab – Databricks Workflow Design

## Objective
Design a Databricks Workflow to process sales, customer, and product datasets with parallel ingestion, data quality checks, parameterization, looping, scheduling, and notifications.

---

## Workflow Overview

### 1. Job & Database
- **Job Name:** Sales Analytics Challenge - [Your Name]
- **Database:** `module2_db`
- **Cluster:** New job cluster

### 2. Tasks

#### T_Enrich_All_Data
- **Joins:** 
    - `bronze_sales` JOIN `bronze_customers` ON `CustomerName`
    - Result JOIN `bronze_products` ON `Item`
- **Calculations:** 
    - `NetRevenue = Quantity * UnitPrice`
- **Output:** 
    - Save to `silver_comprehensive_sales` (Delta table)
    - Set task values: 
        - `enrichment_status = "SUCCESS"`
        - `processed_record_count = count(silver_comprehensive_sales)`

#### T_IfElse_Enrichment
- **Depends on:** `T_Enrich_All_Data`
- **Condition:** 
    - `enrichment_status == "SUCCESS"` **AND** `processed_record_count > 0`
- **True Branch (Analytics Path):**
    - **T_Aggregate_By_Region_Category:** Aggregate `NetRevenue` by `Region` and `Category`, save to `gold_revenue_by_region_category`
    - **T_Loop_By_CustomerType (For Each):**
        - **Input:** Distinct `CustomerType` values from `bronze_customers`
        - **Sub_Process_CustomerType_Segment:** For each `CustomerType`, filter `silver_comprehensive_sales`, aggregate total and average `NetRevenue` per `SalesOrderNumber`, save to `dbfs:/FileStore/lab_data/challenge_outputs/customer_type_reports/{{item}}/`
- **False Branch:**
    - **T_Notify_Enrichment_Failure:** Log error message, print `processed_record_count` if available

### 3. Job Parameters
- `target_database`: `module2_db`
- `report_date`: `{{current_date}}`

### 4. Scheduling & Notifications
- **Schedule:** Daily
- **Notifications:** Email on job success and critical failures

---

## Deliverables

- **Job JSON Export:** Export the workflow definition as JSON from Databricks Jobs UI.
- **Screenshots:** 
    - Successful run graph
    - If/Else and For Each task configurations
- **Monitoring & Failure Handling:** 
    - Monitor via Databricks Job Runs UI and alert emails
    - On `T_Enrich_All_Data` failure, review logs, check input data, and rerun after fixing issues

---

## Example: Monitoring & Failure Handling

- **Monitoring:** Use Databricks Job Runs UI for status, logs, and metrics. Set up email alerts for failures.
- **Failure Handling:** If `T_Enrich_All_Data` fails, the workflow triggers `T_Notify_Enrichment_Failure` to log the error and notify stakeholders. Investigate logs, validate source data, and rerun after resolving issues.

---

> **Tip:** Parameterize paths and table names for flexibility and reusability.
