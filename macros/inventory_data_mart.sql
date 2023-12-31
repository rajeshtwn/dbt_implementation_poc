{% macro inventory_data_mart(source_model) %}
SELECT
    SRC.MEAS_DT AS MEAS_DT,
    COALESCE(SRC.ITEM_KEY, -1) AS ITEM_KEY,
    COALESCE(SRC.LOCATION_KEY, -1) AS LOCATION_KEY,
    SUM(SRC.F_FACT_QTY) AS F_FACT_QTY,
    SUM(SRC.F_FACT_CST) AS F_FACT_CST,
    SUM(SRC.F_FACT_RTL) AS F_FACT_RTL
FROM {{ source_model }} SRC
GROUP BY
    MEAS_DT,
    COALESCE(SRC.ITEM_KEY, -1),
    COALESCE(SRC.LOCATION_KEY, -1)
{% endmacro %}
