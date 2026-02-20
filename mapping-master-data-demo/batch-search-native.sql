USE SCHEMA AI_DEMOS.MASTER_DATA_MAPPING_DEMO;

-- https://docs.snowflake.com/en/LIMITEDACCESS/cortex-search/batch-cortex-search
SELECT
    q.SOFTWARE_NAME,
    r.*
FROM SOFTWARE_APPLICATIONS_USER_PROVIDED AS q,
LATERAL CORTEX_SEARCH_BATCH(
    service_name => 'AI_DEMOS.MASTER_DATA_MAPPING_DEMO.SOFTWARE_APPS_SEARCH',
    query => q.SOFTWARE_NAME,   -- optional STRING
    limit => 10                -- optional INT
) AS r;