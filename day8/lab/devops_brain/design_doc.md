# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data from both clean and dirty sources, processes it, and stores it in a DuckDB database. It transforms raw transaction data into enriched formats and computes merchant performance and daily summaries.

## Data Flow Diagram

```plaintext
+--------------------+     +-------------------+     +-------------------+     +-------------------+
|  Source            |     |  Bronze Layer      |     |  Silver Layer      |     |  Gold Layer        |
|  (TRANSACTIONS_CLEAN, | --> |  bronze_transactions | --> |  silver_transactions | --> |  gold_merchant_performance, |
|  TRANSACTIONS_DIRTY) |     |                     |     |                     |     |  gold_daily_summary |
+--------------------+     +-------------------+     +-------------------+     +-------------------+
```

## Key Design Decisions
- **Layered Data Storage**: The pipeline uses a three-layer approach (Bronze, Silver, Gold) to ensure data integrity and facilitate complex transformations.
- **Quality Flags**: Introduced quality flags in the Silver layer to distinguish between clean and potentially problematic data.
- **Aggregations in Gold Layer**: Computed metrics like merchant performance and daily summaries in the Gold layer to provide high-level insights.
- **Error Handling**: Ignored errors during merchant data insertion to ensure data consistency.

## Known Limitations
- **Single-Node Database**: The pipeline uses a single DuckDB database instance, which may not scale well for very large datasets.
- **Static Merchant Data**: Merchant data is loaded once and not updated, which could lead to stale information if merchant details change.
- **Limited Error Handling**: Basic error handling is implemented, which may not cover all edge cases.
- **No Data Validation**: The pipeline assumes input data is in the expected format without additional validation.

## Dependencies
- **DuckDB Database**: The pipeline requires a DuckDB database instance to store and process data.
- **MERCHANTS Data**: A predefined list of merchant data is required for enriching transaction records.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY**: Source data files containing transaction records.