SELECT
    'TMP_PRODUCT' as table_name,
    ITEM_ID as primary_key_column,
    COUNT(*) as num_records
FROM DBT_POC_DB.DBT_POC_TMP.TMP_PRODUCT
GROUP BY ITEM_ID
HAVING COUNT(*) > 1

UNION ALL

SELECT
    'TMP_LOCATION' as table_name,
    LOCATION_ID as primary_key_column,
    COUNT(*) as num_records
FROM DBT_POC_DB.DBT_POC_TMP.TMP_LOCATION
GROUP BY LOCATION_ID
HAVING COUNT(*) > 1

UNION ALL

SELECT
    'TMP_SALES' as table_name,
    TXN_ID as primary_key_column,
    COUNT(*) as num_records
FROM DBT_POC_DB.DBT_POC_TMP.TMP_SALES
GROUP BY TXN_ID
HAVING COUNT(*) > 1
