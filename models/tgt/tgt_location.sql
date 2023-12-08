{{
     config(
         materialized='incremental',
         unique_key='LOCATION_ID',
         merge_update_columns=['RCD_UPD_TS', 'NAME', 'ADDRESS1', 'ADDRESS2', 'CITY', 'ZIP', 'PROVINCE', 'COUNTRY', 'PHONE', 'COUNTRY_CODE', 'COUNTRY_NAME', 'PROVINCE_CODE', 'LEGACY', 'ACTIVE', 'LOCALIZED_COUNTRY_NAME', 'LOCALIZED_PROVINCE_NAME'],
     )
}}

{% if is_incremental() %}
SELECT
    COALESCE((SELECT MAX(LOCATION_KEY) FROM {{ this }}), 0) + 1 as LOCATION_KEY,
    LOCATION_ID,
    NAME,
    ADDRESS1,
    ADDRESS2,
    CITY,
    ZIP,
    PROVINCE,
    COUNTRY,
    PHONE,
    COUNTRY_CODE,
    COUNTRY_NAME,
    PROVINCE_CODE,
    LEGACY,
    ACTIVE,
    LOCALIZED_COUNTRY_NAME,
    LOCALIZED_PROVINCE_NAME,
    CURRENT_TIMESTAMP as RCD_INS_TS,
    CURRENT_TIMESTAMP as RCD_UPD_TS,
    'N' as RCD_CLOSE_FLG,
    '9999-12-31' as RCD_CLOSE_DT
FROM {{ ref("tmp_location") }}
{% else %}
SELECT
    ROW_NUMBER() OVER (ORDER BY LOCATION_ID) as LOCATION_KEY,
    LOCATION_ID,
    NAME,
    ADDRESS1,
    ADDRESS2,
    CITY,
    ZIP,
    PROVINCE,
    COUNTRY,
    PHONE,
    COUNTRY_CODE,
    COUNTRY_NAME,
    PROVINCE_CODE,
    LEGACY,
    ACTIVE,
    LOCALIZED_COUNTRY_NAME,
    LOCALIZED_PROVINCE_NAME,
    CURRENT_TIMESTAMP as RCD_INS_TS,
    CURRENT_TIMESTAMP as RCD_UPD_TS,
    'N' as RCD_CLOSE_FLG,
    '9999-12-31' as RCD_CLOSE_DT
FROM {{ ref('tmp_location') }}
{% endif %}