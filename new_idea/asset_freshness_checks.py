from datetime import timedelta

from dagster import build_last_update_freshness_checks
from new_idea.assets import (
    common_orders,
    prep_customers,
    prep_orders,
    prep_reference_data__currency_rates,
)

prep_freshness_check = build_last_update_freshness_checks(
    assets=[
        prep_customers,
        prep_orders,
        prep_reference_data__currency_rates,
    ],
    lower_bound_delta=timedelta(minutes=30),  # Allow check to =True if received within 30 mins before the deadline
    deadline_cron="0 7 * * *",  # Expected by 7am daily
)


common_freshness_check = build_last_update_freshness_checks(
    assets=[common_orders],
    lower_bound_delta=timedelta(minutes=30),  # Allow check to =True if received within 30 mins before the deadline
    deadline_cron="0 9 * * *",  # Expected by 7am daily
)
