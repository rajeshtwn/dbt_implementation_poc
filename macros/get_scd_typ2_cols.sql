{% macro get_scd_typ2_cols() %}
    {% set scd_typ2_col_query = "SELECT SCD_TYP2_COL FROM DBT_POC_DB.DBT_POC_TGT.EDW_D_SCD_CONFIG WHERE SUBJECT_AREA = 'EDW_CUSTOMER';" %}
    {% set scd_typ2_col_result = run_query(scd_typ2_col_query) %}
    
    {% if execute %}
        {% set scd_typ2_col = scd_typ2_col_result.columns[0].values() | list %}
    {% else %}
        {% set scd_typ2_col = [] %}
    {% endif %}
    
    {{ log(scd_typ2_col, true) }}
    
    {{ scd_typ2_col }}


{% endmacro %}
