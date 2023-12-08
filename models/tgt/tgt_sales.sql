{{
     config(
      materialized='incremental',
      unique_key=['TXN_ID'],
      pre_hook=handle_rejection_sales()
     )
}}
{% set table_exists = run_query("SELECT 1 FROM information_schema.tables WHERE table_name = 'REJ_SALES_FOREIGN_KEY' AND table_schema = 'DBT_DBT_TEST__AUDIT'") %}
SELECT
    ITEM_KEY,
    LOCATION_KEY,
    TXN_ID,
    TXN_DT,
    F_SLS_RTL,
    F_SLS_QTY,
    F_SLS_CST,
    current_timestamp as RCD_INS_TS,
    current_timestamp as RCD_UPD_TS
FROM {{ref("tmp_sales")}}
{% if table_exists and table_exists[0][0] == 1 %}
    WHERE TXN_ID NOT IN (SELECT TXN_ID FROM DBT_DBT_TEST__AUDIT.REJ_SALES_FOREIGN_KEY)
{% endif %}




