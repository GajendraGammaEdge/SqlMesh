import yaml
from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator
from pathlib import Path

def load_offers():
    with open(Path("offer_config/offer.yaml")) as f:
        return yaml.safe_load(f) or []

for offer in load_offers():
    offer_id = offer["offer_id"]

    offer_start = str(offer["offer_start_date"])
    offer_end = str(offer["monitoring_end_date"])

    @model(
        name=f"offer_monitoring.{offer_id}",
        kind="INCREMENTAL_BY_TIME_RANGE",
        time_column="evaluation_date",
        grain=["customer_id", "evaluation_date"],
        start=offer_start,
        end=offer_end,
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
    def entrypoint(evaluator: MacroEvaluator, config=offer):
        return evaluator.render(
            "{{ build_offer_model(config) }}",
            config=config,
        )
