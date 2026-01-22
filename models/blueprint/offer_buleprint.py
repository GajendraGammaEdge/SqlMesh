import yaml
from pathlib import Path
from sqlmesh import model

def load_offers():
    with open(Path("offer_config/offer.yaml")) as f:
        print("Path_is_offer")
        return yaml.safe_load(f) or []

def register_offer_model(offer: dict):
    offer_id = offer["offer_id"]
    offer_config = offer

    @model(
        name=f"offer_monitoring.{offer_id}",
        kind="FULL",
        start=str(offer_config["offer_start_date"]),
        cron="@daily",
        columns={
            "customer_id": "text",
            "offer_id": "text",
            "enrollment_date": "date",
            "evaluation_date": "date",
            "min_balance_rule_met": "boolean",
            "min_balance_rule_value": "numeric",
            "debit_card_rule_met": "boolean",
            "debit_card_rule_value": "numeric",
            "is_qualified": "boolean",
            "payout_amount": "numeric",
            "reward_type": "text",
        },
    )
    def entrypoint(context, **kwargs):
        # Get reward configuration
        reward = offer_config.get("reward", {})
        reward_type = reward.get("type", "CASH")
        reward_amount = reward.get("amount", 0)

        # Build SQL query with sample enrollment data
        sql = f"""
        WITH sample_enrollments AS (
            SELECT
                ('CUST_' || generate_series::TEXT)::TEXT AS customer_id,
                '{offer_config["offer_id"]}'::TEXT AS offer_id,
                (DATE '{offer_config["offer_start_date"]}' + ((generate_series % 30) * INTERVAL '1 day'))::DATE AS enrollment_date,
                CURRENT_DATE::DATE AS evaluation_date
            FROM generate_series(1, 10)
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
            {reward_amount}::NUMERIC AS payout_amount,
            '{reward_type}'::TEXT AS reward_type
        FROM sample_enrollments
        """

        return sql

    return entrypoint


# Temporarily disabled - using SQL model instead
# for offer in load_offers():
#     register_offer_model(offer)
