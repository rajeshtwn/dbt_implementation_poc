{% macro generate_update_statement(scd_typ2_col_query) %}
    

    {# Check if scd_typ2_col has elements #}
    {% if scd_typ2_col | length > 0 %}

        {% set update_condition_lst = [] %}

        {# Loop through scd_typ2_col elements #}
        {% for col in scd_typ2_col %}
            {% set _ = update_condition_lst.append(" OR SRC." ~ col ~ " <> TGT." ~ col) %}
        {% endfor %}

        {% set update_condition_str = ' '.join(update_condition_lst) %}

        {% set update_sql = "UPDATE DBT_POC_DB.DBT_POC_TGT.EDW_D_CUSTOMER_ATTRIB TGT SET  TGT.RCD_CLOSE_DT = TIM.DAY_KEY - 1, TGT.RCD_OPEN_FLG = 'N' FROM  DBT_POC_DB.DBT_POC_TMP.TMP_D_CUSTOMER_ATTR SRC CROSS JOIN  (SELECT MAX(DAY_KEY) as day_key FROM dbt_poc_db.dbt_poc_tgt.DWH_D_CURR_TIM_LU) TIM WHERE  SRC.CUS_ID = TGT.CUS_ID  AND TGT.RCD_OPEN_FLG = 'Y' AND ( 1 = 0 " 
                        ~ update_condition_str 
                        ~ ")" 
        %}

        {% set _ = run_query(update_sql) %} 

        {% endif %}

{% endmacro %}
