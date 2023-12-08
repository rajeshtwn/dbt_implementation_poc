{% set load_type_query = 
    "select case when day_key = mth_start_dt then 'FULL' else 'DELTA' END as load_type
    from dbt_poc_db.dbt_poc_tgt.dwh_d_tim_day_lu
    where day_key = (SELECT MAX(DAY_KEY) as day_key FROM dbt_poc_db.dbt_poc_tgt.DWH_D_CURR_TIM_LU)"
%}

{% set load_type_result = run_query(load_type_query) %} 

{% if execute %}
{# Return the first column #}
{% set load_type = load_type_result.columns[0].values()[0] %}
{% else %}
{% set load_type = [] %}
{% endif %}

{% if load_type == 'DELTA' %}
    SELECT 
        LOC.LOCATION_KEY
        ,ITM.ITEM_KEY
        ,LOC.LOCATION_ID
        ,ITM.ITEM_ID
        ,SRC.DAY_ID AS DAY_KEY
        ,SRC.F_OH_QTY
        ,SRC.F_OH_CST
        ,SRC.F_OH_RTL
    FROM {{ref('stg_inventory')}} SRC
    INNER JOIN {{ref('tgt_location')}} LOC
        ON LOC.LOCATION_ID = SRC.LOCATION_ID
    INNER JOIN {{ref('tgt_product')}} ITM
        ON ITM.ITEM_ID = SRC.ITEM_ID
    INNER JOIN DBT_POC_DB.DBT_POC_TGT.TGT_INV_IL_B IL
        ON LOC.LOCATION_KEY = IL.LOCATION_KEY
        AND ITM.ITEM_KEY = IL.ITEM_KEY
    WHERE 1 = 1
        AND SRC.F_OH_QTY <> IL.F_OH_QTY
        AND SRC.F_OH_RTL <> IL.F_OH_RTL
        AND SRC.F_OH_CST <> IL.F_OH_CST
{% else %}
    SELECT 
        LOC.LOCATION_KEY
        ,ITM.ITEM_KEY
        ,LOC.LOCATION_ID
        ,ITM.ITEM_ID
        ,SRC.DAY_ID AS DAY_KEY
        ,SRC.F_OH_QTY
        ,SRC.F_OH_CST
        ,SRC.F_OH_RTL
    FROM {{ref('stg_inventory')}} SRC
    INNER JOIN {{ref('tgt_location')}} LOC
        ON LOC.LOCATION_ID = SRC.LOCATION_ID
    INNER JOIN {{ref('tgt_product')}} ITM
        ON ITM.ITEM_ID = SRC.ITEM_ID
{% endif %}
