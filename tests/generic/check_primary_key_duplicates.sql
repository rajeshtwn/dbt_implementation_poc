check_primary_key_duplicates.sql
{% test check_primary_key_duplicates(model, column_name) %}
    SELECT
        '{{ model }}' as table_name,
        {{ column_name }} as primary_key_column,
        COUNT(1) as num_records
    FROM {{ model }}
    GROUP BY {{ column_name }}
    HAVING COUNT(1) > 1
{% endtest %}