{% set scd_typ2_col_query = "SELECT SCD_TYP2_COL FROM DBT_POC_DB.DBT_POC_TGT.EDW_D_SCD_CONFIG WHERE SUBJECT_AREA = 'EDW_CUSTOMER';" %}
{% set scd_typ2_col_result = run_query(scd_typ2_col_query) %}

{% if execute %}
    {% set scd_typ2_col = scd_typ2_col_result.columns[0].values() | list %}
{% else %}
    {% set scd_typ2_col = [] %}
{% endif %}

{{ log(scd_typ2_col, true) }}


{{
  config(
    materialized='incremental',
    unique_key=  ['city','state_cde','country_cde','loyalty_tier','net_spend_l24'],
    merge_update_columns=['age_qualifier_cde', 'estimated_income', 'first_txn_dt', 'last_txn_dt'],
    pre_hook= generate_update_statement(['city','state_cde','country_cde','loyalty_tier','net_spend_l24']),
    where = ["DBT_INTERNAL_SOURCE.city = DBT_INTERNAL_DEST.city"]
  )
}}

SELECT
    TMP.CUS_ID as CUS_ID,
    COALESCE(
        CUS.CUS_KEY,
        (SELECT MAX(CUS_KEY) FROM DBT_POC_DB.DBT_POC_TGT.EDW_D_CUSTOMER_ATTRIB TGT) + ROW_NUMBER() OVER (ORDER BY tmp.cus_id) 
    ) AS CUS_KEY,
    (SELECT MAX(CUS_SCD_KEY) FROM DBT_POC_DB.DBT_POC_TGT.EDW_D_CUSTOMER_ATTRIB ) + ROW_NUMBER() OVER (ORDER BY tmp.cus_id)  AS CUS_SCD_KEY,
    TMP.AGE_QUALIFIER_CDE,
    TMP.CITY,
    TMP.STATE_CDE,
    TMP.COUNTRY_CDE,
    TMP.CUS_ETHNICITY_CDE,
    TMP.LOYALTY_TIER,
    TMP.NET_SPEND_L24,
    TMP.ESTIMATED_INCOME,
    TMP.FIRST_TXN_DT,
    TMP.LAST_TXN_DT,
    'Y' AS RCD_OPEN_FLG,
    TIM.DAY_KEY AS RCD_OPEN_DT,
    '9999-12-31' AS RCD_CLOSE_DT
FROM 
  DBT_POC_DB.DBT_POC_TMP.tmp_d_customer_attrib TMP
LEFT JOIN 
  (
    SELECT DISTINCT CUS_ID, CUS_KEY, city, state_cde, country_cde, loyalty_tier, net_spend_l24
    FROM DBT_POC_DB.DBT_POC_TGT.EDW_D_CUSTOMER_ATTRIB TGT 
    WHERE RCD_OPEN_FLG = 'Y'
  ) CUS
ON 
  TMP.CUS_ID = CUS.CUS_ID
CROSS JOIN 
(SELECT MAX(DAY_KEY) as day_key FROM dbt_poc_db.dbt_poc_tgt.DWH_D_CURR_TIM_LU) TIM
