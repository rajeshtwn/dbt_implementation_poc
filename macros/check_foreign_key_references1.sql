check_foreign_key_references1.sql

{% test check_foreign_key_references1(model, column_name, field, to) %}

    {% set column_list = column_name.split(',') %}
    {% set field_list = field.split(',') %}
    {% set to_list = to %}

    {% set n = field_list|length %}

    {% if n == 1 %}
      SELECT DISTINCT '{{ to }}', {{ column_name }} 
      FROM {{ model }}
      WHERE {{ column_name }} IS NOT NULL 
        AND {{ column_name }} NOT IN (
          SELECT {{ field }} 
          FROM {{ to }}
        )
    
    {% else %}

      SELECT DISTINCT '{{ to_list[0] }}', {{ column_list[0] }} 
      FROM {{ model }}
      WHERE {{ column_list[0] }} IS NOT NULL 
        AND {{ column_list[0] }} NOT IN (
          SELECT {{ field_list[0] }} 
          FROM {{ to_list[0] }}
        )

        {% for i in range(1, n) %}
          UNION ALL
          SELECT DISTINCT '{{ to_list[i] }}', {{ column_list[i] }} 
          FROM {{ model }}
          WHERE {{ column_list[i] }} IS NOT NULL 
            AND {{ column_list[i] }} NOT IN (
              SELECT {{ field_list[i] }} 
              FROM {{ to_list[i] }}
            )
        {% endfor %}
    {% endif %}

{% endtest %}
