{% macro build_qualification_expr(q) %}
  {% set parts = [] %}
  {% for c in q.conditions %}
    {% do parts.append(c.rule_id ~ '_met = TRUE') %}
  {% endfor %}
  ({{ parts | join(' ' ~ q.operator ~ ' ') }})
{% endmacro %}
