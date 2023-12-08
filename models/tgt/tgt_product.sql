{{
     config(
         materialized='incremental',
         unique_key='ITEM_ID',
         merge_update_columns=['PRODUCT_ID', 'RCD_UPD_TS', 'TITLE','PRICE', 'SKU',  'POSITION', 'INVENTORY_POLICY', 'FULFILLMENT_SERVICE', 'INVENTORY_MANAGEMENT', 'TAXABLE' ,'WEIGHT', 'WEIGHT_UNIT', 'REQUIRES_SHIPPING'],
     )
}}
{% if is_incremental() %}
SELECT
    COALESCE((SELECT MAX(ITEM_KEY) FROM {{ this }}), 0) + 1 as ITEM_KEY,
    ITEM_ID,
    PRODUCT_ID,
    TITLE,
    PRICE,
    SKU,
    POSITION,
    INVENTORY_POLICY,
    FULFILLMENT_SERVICE,
    INVENTORY_MANAGEMENT,
    TAXABLE,
    WEIGHT,
    WEIGHT_UNIT,
    REQUIRES_SHIPPING,
    current_timestamp as RCD_INS_TS,
    current_timestamp as RCD_UPD_TS,
    'N' as RCD_CLOSE_FLG,
    '9999-12-31' as RCD_CLOSE_DT
FROM {{ref("tmp_product")}}
{% else %}
  select
    row_number() over (order by ITEM_ID) as ITEM_KEY,
    ITEM_ID,
    PRODUCT_ID,
    TITLE,
    PRICE,
    SKU,
    POSITION,
    INVENTORY_POLICY,
    FULFILLMENT_SERVICE,
    INVENTORY_MANAGEMENT,
    TAXABLE,
    WEIGHT,
    WEIGHT_UNIT,
    REQUIRES_SHIPPING,
    current_timestamp as RCD_INS_TS,
    current_timestamp as RCD_UPD_TS,
    'N' as RCD_CLOSE_FLG,
    '9999-12-31' as RCD_CLOSE_DT
  from {{ ref('tmp_product') }}
{% endif %}
