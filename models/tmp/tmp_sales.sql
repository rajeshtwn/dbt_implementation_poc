{{
  config(
  materialized='table',
  post_hook= reprocess_rejection_sales('DBT_POC_DB.DBT_POC_TMP.tmp_sales')
  )
}}
WITH temp_sales AS
(
  SELECT loc.LOCATION_KEY, pd.ITEM_KEY, sls.* FROM {{ref('stg_sales')}} sls
  INNER JOIN DBT_POC_TGT.TGT_LOCATION loc ON loc.LOCATION_ID = sls.LOCATION_ID
  INNER JOIN DBT_POC_TGT.TGT_PRODUCT pd ON pd.ITEM_ID = sls.PRODUCT_ID
)
SELECT * FROM temp_sales

