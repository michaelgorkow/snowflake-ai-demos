# Master Data Mapping Demo

A demonstration of using **Cortex Search** to intelligently map user-provided data to a canonical master data set. This is a common enterprise challenge where incoming data contains variations, abbreviations, typos, or inconsistent naming that needs to be reconciled against a golden source of truth.

## Overview

This demo showcases how to leverage Snowflake's Cortex Search to perform fuzzy matching at scale. The use case involves mapping software application names (with variations like "MS Teams", "ZOOM", "git hub") to their canonical master records ("Microsoft Teams", "Zoom", "GitHub").

### Key Features

- **Semantic Matching**: Uses vector embeddings to find matches even with significant variations
- **Batch Processing**: Efficiently processes large volumes of records
- **Scoring Metrics**: Returns cosine similarity and text match scores for match quality assessment
- **Handles Edge Cases**: Identifies records that don't exist in the master data

## Requirements
This demo assumes that you have setup your environment using the `environment.setup.sql`.
At a minimum you need to have privileges creating data, Cortex Search Services and Stored Procedures.

## Demo Scripts

| Script | Description |
|--------|-------------|
| `data-generation.sql` | Creates sample master data and user-provided data, then builds the Cortex Search service |
| `batch-search-sproc.sql` | Batch search implementation using a stored procedure (general availability) |
| `batch-search-native.sql` | Batch search using native `CORTEX_SEARCH_BATCH` function (private preview) |

## Quick Start

### Step 1: Generate Sample Data

Run `data-generation.sql` to set up the demo environment:

This creates a master table with 100 software applications and a user-provided table with intentional variations such as:
- Case differences: `sap` → `SAP`
- Abbreviations: `MS Teams` → `Microsoft Teams`
- Spacing issues: `snow flake` → `Snowflake`
- Missing from master: `Linear`, `Basecamp`, `Clockify`

### Step 2: Run Batch Search

Choose one of the following approaches:

#### Option A: Stored Procedure

Run `batch-search-sproc.sql` - Works in all Snowflake accounts:

```sql
-- Calls the batch_cortex_search stored procedure
CALL batch_cortex_search(
    'AI_DEMOS',
    'MASTER_DATA_MAPPING_DEMO',
    'SOFTWARE_APPS_SEARCH',
    (SELECT ARRAY_AGG(SOFTWARE_NAME) FROM SOFTWARE_APPLICATIONS_USER_PROVIDED),
    (SELECT ARRAY_AGG({}) FROM SOFTWARE_APPLICATIONS_USER_PROVIDED),
    ARRAY_CONSTRUCT('SOFTWARE_NAME'),
    -1
);
```

#### Option B: Native Function (Private Preview)

Run `batch-search-native.sql` - Requires private preview access:

```sql
SELECT q.SOFTWARE_NAME, r.*
FROM SOFTWARE_APPLICATIONS_USER_PROVIDED AS q,
LATERAL CORTEX_SEARCH_BATCH(
    service_name => 'AI_DEMOS.MASTER_DATA_MAPPING_DEMO.SOFTWARE_APPS_SEARCH',
    query => q.SOFTWARE_NAME,
    limit => 10
) AS r;
```

### Step 3: View Results

The stored procedure approach includes a query to format results:

```sql
SELECT
    value['query']::TEXT as USER_TABLE_RECORD,
    value['results'][0]['SOFTWARE_NAME']::TEXT as MASTER_TABLE_RECORD,
    value['results'][0]['@scores']['cosine_similarity']::FLOAT as COSINE_SIMILARITY,
    value['results'][0]['@scores']['text_match']::FLOAT as TEXT_MATCH_SCORE
FROM RESULTS r, LATERAL FLATTEN(r.batch_cortex_search) 
ORDER BY COSINE_SIMILARITY DESC;
```

## Sample Output

| User Record | Master Match | Cosine Similarity |
|-------------|--------------|-------------------|
| MS Teams | Microsoft Teams | 0.92 |
| snow flake | Snowflake | 0.89 |
| Azure | Microsoft Azure | 0.88 |
| git hub | GitHub | 0.85 |

## Use Cases

- **Data Cleansing**: Standardize incoming data against master records
- **Deduplication**: Identify potential duplicate entries
- **Data Integration**: Map data from multiple sources to a unified schema
- **Product Catalog Matching**: Match vendor product names to internal SKUs
- **Customer MDM**: Reconcile customer records across systems

## Resources

- [Cortex Search Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
