{% macro minimum_balance_rule(rule_id, offer_id, params, offer_start, offer_end) %}

{% set account_types = params.get('account_types', ['CHECKING']) %}
{% set operator = params.get('operator', '>=') %}
{% set threshold = params.get('threshold', 0) %}
{% set days = params.get('monitoring_period_days', 30) %}

SELECT
  e.customer_id,
  e.offer_id,
  '{{ rule_id }}' AS rule_id,
  e.enrollment_date,
  @end_date::date AS evaluation_date,
  MIN(b.balance) AS measured_value,
  CASE
    WHEN MIN(b.balance) {{ operator }} {{ threshold }}
    THEN TRUE ELSE FALSE
  END AS meets_condition
FROM offer_enrollments e
JOIN daily_balances b
  ON e.customer_id = b.customer_id
 AND b.balance_date BETWEEN e.enrollment_date
 AND e.enrollment_date + INTERVAL '{{ days }} days'
 AND b.account_type IN (
   {% for t in account_types %}'{{ t }}'{% if not loop.last %}, {% endif %}{% endfor %}
 )
WHERE e.offer_id = '{{ offer_id }}'
  AND e.enrollment_date BETWEEN '{{ offer_start }}' AND '{{ offer_end }}'
  AND e.enrollment_date + INTERVAL '{{ days }} days' = @end_date
GROUP BY e.customer_id, e.offer_id, e.enrollment_date

{% endmacro %}
