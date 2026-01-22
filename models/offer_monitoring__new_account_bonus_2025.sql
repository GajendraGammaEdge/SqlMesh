MODEL (
  name offer_monitoring.new_account_bonus_2025,
  kind FULL,
  start '2025-01-01',
  cron '@daily',
  grain (customer_id, offer_id, enrollment_date)
);

-- Offer monitoring model for new_account_bonus_2025
WITH numbers AS (
    SELECT customer_num
    FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) AS t(customer_num)
),
sample_enrollments AS (
    SELECT
        ('CUST_' || customer_num::TEXT)::TEXT AS customer_id,
        'new_account_bonus_2025'::TEXT AS offer_id,
        (DATE '2025-01-01' + ((customer_num % 30) * INTERVAL '1 day'))::DATE AS enrollment_date,
        CURRENT_DATE::DATE AS evaluation_date
    FROM numbers
)
SELECT
    customer_id,
    offer_id,
    enrollment_date,
    evaluation_date,
    FALSE AS min_balance_rule_met,
    0::NUMERIC AS min_balance_rule_value,
    FALSE AS debit_card_rule_met,
    0::NUMERIC AS debit_card_rule_value,
    FALSE AS is_qualified,
    200::NUMERIC AS payout_amount,
    'CASH'::TEXT AS reward_type
FROM sample_enrollments
