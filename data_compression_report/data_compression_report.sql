USE DATABASE UDACITYDWHPROJECT;

WITH stage_files AS (
    SELECT 
        'Stage' as storage_type,
        REGEXP_REPLACE(RELATIVE_PATH, '^.*/([^/]+)$', '\\1') as file_name,
        SIZE as file_size_bytes,
        ROUND(SIZE / POWER(1024, 2), 2) as file_size_mb,
        ROUND(SIZE / POWER(1024, 3), 2) as file_size_gb
    FROM DIRECTORY(@UDACITYDWHPROJECT.STAGING."BUSINESS")
    WHERE RELATIVE_PATH IS NOT NULL
    UNION ALL
    SELECT 
        'Stage' as storage_type,
        REGEXP_REPLACE(RELATIVE_PATH, '^.*/([^/]+)$', '\\1') as file_name,
        SIZE as file_size_bytes,
        ROUND(SIZE / POWER(1024, 2), 2) as file_size_mb,
        ROUND(SIZE / POWER(1024, 3), 2) as file_size_gb
    FROM DIRECTORY(@UDACITYDWHPROJECT.STAGING."CHECK_IN")
    WHERE RELATIVE_PATH IS NOT NULL
    UNION ALL
    SELECT 
        'Stage' as storage_type,
        REGEXP_REPLACE(RELATIVE_PATH, '^.*/([^/]+)$', '\\1') as file_name,
        SIZE as file_size_bytes,
        ROUND(SIZE / POWER(1024, 2), 2) as file_size_mb,
        ROUND(SIZE / POWER(1024, 3), 2) as file_size_gb
    FROM DIRECTORY(@UDACITYDWHPROJECT.STAGING."COVID")
    WHERE RELATIVE_PATH IS NOT NULL
    UNION ALL
    SELECT 
        'Stage' as storage_type,
        REGEXP_REPLACE(RELATIVE_PATH, '^.*/([^/]+)$', '\\1') as file_name,
        SIZE as file_size_bytes,
        ROUND(SIZE / POWER(1024, 2), 2) as file_size_mb,
        ROUND(SIZE / POWER(1024, 3), 2) as file_size_gb
    FROM DIRECTORY(@UDACITYDWHPROJECT.STAGING."CUSTOMER")
    WHERE RELATIVE_PATH IS NOT NULL
    UNION ALL
    SELECT 
        'Stage' as storage_type,
        REGEXP_REPLACE(RELATIVE_PATH, '^.*/([^/]+)$', '\\1') as file_name,
        SIZE as file_size_bytes,
        ROUND(SIZE / POWER(1024, 2), 2) as file_size_mb,
        ROUND(SIZE / POWER(1024, 3), 2) as file_size_gb
    FROM DIRECTORY(@UDACITYDWHPROJECT.STAGING."PRECIPITATION")
    WHERE RELATIVE_PATH IS NOT NULL
    UNION ALL
    SELECT 
        'Stage' as storage_type,
        REGEXP_REPLACE(RELATIVE_PATH, '^.*/([^/]+)$', '\\1') as file_name,
        SIZE as file_size_bytes,
        ROUND(SIZE / POWER(1024, 2), 2) as file_size_mb,
        ROUND(SIZE / POWER(1024, 3), 2) as file_size_gb
    FROM DIRECTORY(@UDACITYDWHPROJECT.STAGING."REVIEW")
    WHERE RELATIVE_PATH IS NOT NULL
    UNION ALL
    SELECT 
        'Stage' as storage_type,
        REGEXP_REPLACE(RELATIVE_PATH, '^.*/([^/]+)$', '\\1') as file_name,
        SIZE as file_size_bytes,
        ROUND(SIZE / POWER(1024, 2), 2) as file_size_mb,
        ROUND(SIZE / POWER(1024, 3), 2) as file_size_gb
    FROM DIRECTORY(@UDACITYDWHPROJECT.STAGING."TEMPERATURE")
    WHERE RELATIVE_PATH IS NOT NULL
    UNION ALL
    SELECT 
        'Stage' as storage_type,
        REGEXP_REPLACE(RELATIVE_PATH, '^.*/([^/]+)$', '\\1') as file_name,
        SIZE as file_size_bytes,
        ROUND(SIZE / POWER(1024, 2), 2) as file_size_mb,
        ROUND(SIZE / POWER(1024, 3), 2) as file_size_gb
    FROM DIRECTORY(@UDACITYDWHPROJECT.STAGING."TIPS")
    WHERE RELATIVE_PATH IS NOT NULL
),

table_sizes_staging AS (
    SELECT 
        TABLE_SCHEMA as storage_type,
        TABLE_NAME AS name,
        ROW_COUNT as row_count,  -- Removed quotes
        BYTES as size_bytes,  -- Changed to ACTIVE_BYTES
        ROUND(BYTES / POWER(1024, 2), 2) as size_mb,
        ROUND(BYTES / POWER(1024, 3), 2) as size_gb
    FROM "INFORMATION_SCHEMA".TABLES  -- Changed to TABLES view
    WHERE TABLE_SCHEMA = 'STAGING'
),

table_sizes_ods AS (
    SELECT 
        TABLE_SCHEMA as storage_type,
        TABLE_NAME AS name,
        ROW_COUNT as row_count,  -- Removed quotes
        BYTES as size_bytes,  -- Changed to ACTIVE_BYTES
        ROUND(BYTES / POWER(1024, 2), 2) as size_mb,
        ROUND(BYTES / POWER(1024, 3), 2) as size_gb
    FROM "INFORMATION_SCHEMA".TABLES  -- Changed to TABLES view
    WHERE TABLE_SCHEMA = 'ODS'
)


-- Combine stage files and table metrics
SELECT 
    storage_type,
    file_name,
    file_size_bytes,
    file_size_mb,
    file_size_gb
FROM stage_files

UNION ALL

SELECT 
    storage_type,
    name as file_name,
    size_bytes as file_size_bytes,
    size_mb as file_size_mb,
    size_gb as file_size_gb
FROM table_sizes_staging

UNION ALL

SELECT 
    storage_type,
    name as file_name,
    size_bytes as file_size_bytes,
    size_mb as file_size_mb,
    size_gb as file_size_gb
FROM table_sizes_ods
ORDER BY storage_type, file_size_bytes DESC;