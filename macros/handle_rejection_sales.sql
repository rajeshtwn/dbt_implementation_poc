{% macro handle_rejection_sales() %}
    INSERT INTO DBT_POC_DB.DBT_POC_REJ.REJ_SALES
    SELECT *
    FROM DBT_POC_DB.DBT_DBT_TEST__AUDIT.REJ_SALES_FOREIGN_KEY;
{% endmacro %}
