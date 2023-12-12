relationships1.sql

{% test relationships1(model, column_name, field, to, field1, to1) %}

    {% set column_list = column_name.split(',') %}

    SELECT DISTINCT '{{ to }}', {{ column_list[0] }} 
    FROM {{ model }}
    WHERE {{ column_list[0] }} IS NOT NULL 
      AND {{ column_list[0] }} NOT IN (
        SELECT {{ field }} 
        FROM {{ to }}
      )
    UNION
    SELECT DISTINCT '{{ to1 }}', {{ column_list[1] }} 
    FROM {{ model }}
    WHERE {{ column_list[1] }} IS NOT NULL 
      AND {{ column_list[1] }} NOT IN (
        SELECT {{ field1 }} 
        FROM {{ to1 }}
      )

{% endtest %}
