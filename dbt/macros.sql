-- Macros
{% macro left_join(source_name, alias, join_condition) %}
  LEFT JOIN {{ source('warehouse_sales', source_name) }} AS {{ alias }}
    ON {{ join_condition }}
{% endmacro %}


{% macro countif(expression) %}
  COUNTIF({{ expression }})
{% endmacro %}