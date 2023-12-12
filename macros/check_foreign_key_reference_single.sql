check_foreign_key_reference_single.sql
{% test check_foreign_key_reference_single(fk_table_name, fk_column_name, pk_table_name, pk_column_name) %}
    SELECT DISTINCT {{ fk_column_name }} 
    FROM {{ fk_table_name }}
    WHERE ({{ fk_column_name }}) NOT IN (
        SELECT {{ pk_column_name }} FROM {{ pk_table_name }}
)
{% endtest %}
