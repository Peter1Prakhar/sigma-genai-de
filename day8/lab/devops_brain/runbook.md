# Pipeline Overview

This pipeline processes transaction data, transforms it into a cleaned and enriched format, and computes merchant performance and daily summaries. It runs to ensure data is up-to-date for reporting and analytics. If it stops, downstream reports and dashboards will become stale.

## Pipeline Steps

1. Connect to the DuckDB database using `get_connection()`.
2. Set up necessary tables using `setup_tables()`.
3. Load merchant data into the `merchants` table using `load_merchants()`.
4. Load raw transactions into the `bronze_transactions` table using `load_bronze()`.
5. Transform bronze transactions to silver using `transform_bronze_to_silver()`.
6. Load transformed transactions into the `silver_transactions` table using `load_silver()`.
7. Compute merchant performance metrics using `compute_merchant_performance()`.
8. Compute daily summary metrics using `compute_daily_summary()`.
9. Load computed metrics into the `gold_merchant_performance` and `gold_daily_summary` tables using `load_gold()`.

## Schedule / Trigger

This pipeline runs every night at 2 AM UTC via a cron job.

## Failure Modes

1. **Database Connection Failure**
   - **Root Cause:** DuckDB service is down.
   - **Symptom:** `get_connection()` fails.
2. **Table Creation Failure**
   - **Root Cause:** Syntax error in SQL.
   - **Symptom:** `setup_tables()` throws an exception.
3. **Merchant Data Load Failure**
   - **Root Cause:** Corrupt merchant data.
   - **Symptom:** `load_merchants()` fails to insert records.
4. **Bronze Transaction Load Failure**
   - **Root Cause:** Malformed transaction data.
   - **Symptom:** `load_bronze()` fails to insert records.
5. **Silver Transformation Failure**
   - **Root Cause:** Missing merchant IDs in transactions.
   - **Symptom:** `transform_bronze_to_silver()` produces incomplete records.

## Recovery Actions

1. **Database Connection Failure**
   - Notify Platform Manager: `kavya.reddy@sigmadatatech.in`
   - Check DuckDB service status.
   - Restart DuckDB service if necessary.
2. **Table Creation Failure**
   - Review SQL syntax in `setup_tables()`.
   - Correct the SQL and rerun the pipeline.
3. **Merchant Data Load Failure**
   - Inspect `MERCHANTS` data for corruption.
   - Clean the data and rerun `load_merchants()`.
4. **Bronze Transaction Load Failure**
   - Inspect `TRANSACTIONS_CLEAN` and `TRANSACTIONS_DIRTY` for malformed records.
   - Clean the data and rerun `load_bronze()`.
5. **Silver Transformation Failure**
   - Ensure all transactions have valid `merchant_id`.
   - Clean the data and rerun `transform_bronze_to_silver()`.

## Known Bugs

- Hardcoded AWS credentials in the code.
- Lack of null handling in `transform_bronze_to_silver()`.

## Escalation Contacts

1. On-call DE: Priya Nair (`priya.nair@sigmadatatech.in`, +91-98400-11111)
2. Tech Lead: Arjun Mehta (`arjun.mehta@sigmadatatech.in`)
3. Platform Manager: Kavya Reddy (`kavya.reddy@sigmadatatech.in`)

## Data Quality Checks

- Verify the count of records in `bronze_transactions`, `silver_transactions`, `gold_merchant_performance`, and `gold_daily_summary`.
- Ensure `quality_flag` is set correctly in `silver_transactions`.
- Check for duplicate `transaction_id` in `silver_transactions`.
- Validate the computed metrics in `gold_merchant_performance` and `gold_daily_summary`.