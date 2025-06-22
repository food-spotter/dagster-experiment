from dagster import AssetSelection, AutomationCondition, asset, get_dagster_logger


# ================= RAW ASSETS =================
# These will be materialized by some job definition that runs on a schedule.
# For this toy example, we can manually materialize it in the Dagster UI.
@asset(group_name="raw")
def raw_orders():
    pass


@asset(group_name="raw")
def raw_customers():
    pass


@asset(group_name="raw")
def raw_reference_data__currency_rates():
    pass


# ================= PREP ASSETS =================
# Once the upstream raw data is available, we can run this dbt model in an event driven manner
@asset(deps=["raw_orders"], group_name="dbt_prep", compute_kind="dbt", automation_condition=AutomationCondition.eager())
def prep_orders():
    pass


@asset(
    deps=["raw_customers"], group_name="dbt_prep", compute_kind="dbt", automation_condition=AutomationCondition.eager()
)
def prep_customers():
    pass


@asset(
    deps=["raw_reference_data__currency_rates"],
    group_name="dbt_prep",
    compute_kind="dbt",
    automation_condition=AutomationCondition.eager(),
)
def prep_reference_data__currency_rates():
    pass


# ================= STG ASSETS =================
# We only want it to run when downstream assets require it.
@asset(
    deps=["prep_orders"],
    group_name="dbt_staging",
    compute_kind="dbt",
    automation_condition=AutomationCondition.any_downstream_conditions(),
)
def stg_orders():
    pass


@asset(
    deps=["prep_customers"],
    group_name="dbt_staging",
    compute_kind="dbt",
    automation_condition=AutomationCondition.any_downstream_conditions(),
)
def stg_customers():
    pass


@asset(
    deps=["prep_reference_data__currency_rates"],
    group_name="dbt_staging",
    compute_kind="dbt",
    automation_condition=AutomationCondition.any_downstream_conditions(),
)
def stg_reference_data__currency_rates():
    pass


# ================= INT ASSETS =================
# We only want it to run when downstream assets require it.
@asset(
    deps=["stg_orders"],
    group_name="dbt_intermediate",
    compute_kind="dbt",
    automation_condition=AutomationCondition.any_downstream_conditions(),
)
def int_todays_orders():
    pass


@asset(
    deps=["stg_customers"],
    group_name="dbt_intermediate",
    compute_kind="dbt",
    automation_condition=AutomationCondition.any_downstream_conditions(),
)
def int_top_customers():
    pass


@asset(
    deps=["stg_orders", "stg_customers"],
    group_name="dbt_intermediate",
    compute_kind="dbt",
    automation_condition=AutomationCondition.any_downstream_conditions(),
)
def int_customer_orders():
    pass


# ================= COMMON ASSETS =================
# Consider these curated key business model that is used by multiple downstream assets.
# We only want it to run when downstream assets require it.
# We only want it to run when the cron schedule is met and the upstream assets are available.
# This ensures our common model is run only when the upstream data is ready even if it arrives later than expected

target_assets = AssetSelection.assets(
    "int_customer_orders",
    "stg_reference_data__currency_rates",
).downstream(include_self=False)

common_automation_condition = AutomationCondition.on_cron("50 8 * * *").allow(
    target_assets
) & AutomationCondition.all_deps_blocking_checks_passed().allow(target_assets)


@asset(
    deps=["int_customer_orders", "stg_reference_data__currency_rates"],
    group_name="dbt_common",
    compute_kind="dbt",
    automation_condition=common_automation_condition,
)
def common_orders():
    pass


# ================= MART ASSETS =================
# This is a mart model. We only want it to run when the cron schedule is met
# and the upstream assets are available. This ensures our mart is run only when
# the upstream data is ready even if it arrives later than expected

prep_downstream_assets = AssetSelection.groups("dbt_prep").downstream(include_self=True)
common_upstream_assets = AssetSelection.groups("dbt_common").upstream(include_self=True)
# intersection_assets = prep_downstream_assets & common_upstream_assets
target_assets = prep_downstream_assets - common_upstream_assets

mart_automation_condition = AutomationCondition.on_cron("0 10 * * *").allow(
    target_assets
) & AutomationCondition.all_deps_blocking_checks_passed().allow(target_assets)


@asset(
    deps=["common_orders", "int_top_customers"],
    group_name="dbt_mart",
    compute_kind="dbt",
    automation_condition=mart_automation_condition,
)
def mart_top_eu_customers(context):
    pass
