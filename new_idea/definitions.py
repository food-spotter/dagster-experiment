from dagster import (
    Definitions,
    build_sensor_for_freshness_checks,
    load_asset_checks_from_modules,
    load_assets_from_modules,
)
from new_idea import asset_freshness_checks, assets  # noqa: TID252

all_assets = load_assets_from_modules([assets])
all_asset_freshness_checks = load_asset_checks_from_modules([asset_freshness_checks])

# Define freshness check sensor
freshness_checks_sensor = build_sensor_for_freshness_checks(freshness_checks=all_asset_freshness_checks)

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_freshness_checks,
    sensors=[freshness_checks_sensor],
)
