check_foreign_key_references.sql

{% test check_foreign_key_references(model, column_name, field, to) %}

    {% set column_list = column_name.split(',') %}
    {% set field_list = field.split(',') %}
    {% set to_list = to %}

    SELECT DISTINCT '{{ to_list[0] }}', {{ column_list[0] }} 
    FROM {{ model }}
    WHERE {{ column_list[0] }} IS NOT NULL 
      AND {{ column_list[0] }} NOT IN (
        SELECT {{ field_list[0] }} 
        FROM {{ to_list[0] }}
      )
    UNION ALL
    SELECT DISTINCT '{{ to_list[1] }}', {{ column_list[1] }} 
    FROM {{ model }}
    WHERE {{ column_list[1] }} IS NOT NULL 
      AND {{ column_list[1] }} NOT IN (
        SELECT {{ field_list[1] }} 
        FROM {{ to_list[1] }}
      )

{% endtest %}
