{% macro render_rule(rule, offer_id, offer_start, offer_end) %}
  {% if rule.rule_type == 'MinimumBalanceMonitoring' %}
    {{ minimum_balance_rule(rule.rule_id, offer_id, rule.rule_parameters, offer_start, offer_end) }}
  {% elif rule.rule_type == 'NumberOfTransactionsMonitoring' %}
    {{ transaction_count_rule(rule.rule_id, offer_id, rule.rule_parameters, offer_start, offer_end) }}
  {% endif %}
{% endmacro %}
