# Data Quality Checks - Silver Layer

## Overview

This repository contains SQL scripts for performing data quality checks on the **Silver layer** in a **Medallion Architecture** data pipeline. These checks ensure data consistency, accuracy, and standardization before progressing to the Gold layer where business-ready views will be created.

![Medallion Architecture](https://databricks.com/wp-content/uploads/2021/08/Delta-Medallion-Architecture.png)

## Architecture Context

- **Bronze**: Raw ingested data (not modified)
- **Silver**: Cleaned, validated, and enriched data (this layer)
- **Gold**: Business-ready aggregated views (next step)

## Quality Checks Performed

1. **Primary Key Validation**
   - Null checks
   - Duplicate detection
2. **Data Integrity Checks**
   - Invalid date ranges
   - Chronological consistency (order dates vs ship dates)
   - Mathematical consistency (sales = quantity × price)
3. **Standardization Checks**
   - Unwanted whitespace
   - Valid domain values (marital status, gender codes, etc.)
   - Negative values where prohibited

## Tables Validated

| Table Name                 | Description           | Key Checks                          |
| -------------------------- | --------------------- | ----------------------------------- |
| `silver.crm_cust_info`     | Customer demographics | PK, marital status values, trimming |
| `silver.crm_prd_info`      | Product catalog       | PK, cost validation, date ranges    |
| `silver.crm_sales_details` | Transaction records   | Date logic, sales calculations      |
| `silver.erp_cust_az12`     | ERP customer data     | Birthdate ranges, gender codes      |
| `silver.erp_loc_a101`      | Location data         | Country code standardization        |
| `silver.erp_px_cat_g1v2`   | Product categories    | Trimming, maintenance flags         |

## Usage Instructions

```sql
-- Example: Running quality checks for customer data
SELECT
    cst_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
```
