{% macro close_dimension(target_model, source_model, unique_column) %}
UPDATE {{ target_model }}
SET
  RCD_CLOSE_FLG = 'Y',
  RCD_CLOSE_DT = current_date()
WHERE {{ target_model }}.{{ unique_column }} NOT IN (SELECT {{ unique_column }} FROM {{ source_model }});
{% endmacro %}
