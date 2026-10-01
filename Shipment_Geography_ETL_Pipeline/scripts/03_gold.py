from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from sg_common import (
    begin_stage,
    clean_layer_files,
    create_run_id,
    end_stage,
    ensure_dirs,
    get_logger,
    load_settings,
    read_mapping_contract,
    read_parquet,
    target_table_from_mapping,
    write_quality_check,
    write_stage_metric,
    write_parquet,
)


REQUIRED_TABLES = [
    "shipment",
    "customer",
    "office",
    "pin_code",
    "city",
    "zone",
    "state",
    "region",
    "country",
]


def _required_columns(df: pd.DataFrame, table_name: str, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns in {table_name}: {missing}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold shipment_geography dataset from silver tables.")
    parser.add_argument("--run-id", default="", help="Optional run id shared across all pipeline stages.")
    return parser.parse_args()


def _build_shipment_geography_frame(silver_layer_dir: Path) -> pd.DataFrame:
    shipment = read_parquet(silver_layer_dir, "shipment")
    customer = read_parquet(silver_layer_dir, "customer")
    office = read_parquet(silver_layer_dir, "office")
    pin_code = read_parquet(silver_layer_dir, "pin_code")
    city = read_parquet(silver_layer_dir, "city")
    zone = read_parquet(silver_layer_dir, "zone")
    state = read_parquet(silver_layer_dir, "state")
    region = read_parquet(silver_layer_dir, "region")
    country = read_parquet(silver_layer_dir, "country")

    _required_columns(shipment, "shipment", ["shipment_id", "customer_id", "office_id"])
    _required_columns(customer, "customer", ["customer_id", "customer_name"])
    _required_columns(office, "office", ["office_id", "office_name", "pincode_id"])
    _required_columns(pin_code, "pin_code", ["pincode_id", "pincode", "city_id"])
    _required_columns(city, "city", ["city_id", "city_name", "zone_id", "state_id"])
    _required_columns(zone, "zone", ["zone_id", "zone_name"])
    _required_columns(state, "state", ["state_id", "state_name", "region_id"])
    _required_columns(region, "region", ["region_id", "region_name", "country_id"])
    _required_columns(country, "country", ["country_id", "country_name"])

    # Follow the same lookup chain that is documented in the mapping CSV.
    frame = shipment[["shipment_id", "customer_id", "office_id"]].copy()
    frame = frame.merge(customer[["customer_id", "customer_name"]], on="customer_id", how="left")
    frame = frame.merge(office[["office_id", "office_name", "pincode_id"]], on="office_id", how="left")
    frame = frame.merge(pin_code[["pincode_id", "pincode", "city_id"]], on="pincode_id", how="left")
    frame = frame.merge(city[["city_id", "city_name", "zone_id", "state_id"]], on="city_id", how="left")
    frame = frame.merge(zone[["zone_id", "zone_name"]], on="zone_id", how="left")
    frame = frame.merge(state[["state_id", "state_name", "region_id"]], on="state_id", how="left")
    frame = frame.merge(region[["region_id", "region_name", "country_id"]], on="region_id", how="left")
    frame = frame.merge(country[["country_id", "country_name"]], on="country_id", how="left")

    final_cols = [
        "shipment_id",
        "customer_name",
        "office_name",
        "pincode",
        "city_name",
        "zone_name",
        "state_name",
        "region_name",
        "country_name",
    ]
    return frame[final_cols].copy()


def _check_duplicate_keys(df: pd.DataFrame, key_col: str) -> int:
    if key_col not in df.columns:
        return 0
    return int(df.duplicated(subset=[key_col]).sum())


def _check_null_keys(df: pd.DataFrame, key_col: str) -> int:
    if key_col not in df.columns:
        return 0
    return int(df[key_col].isna().sum())

def main() -> None:
    """Stage 3: join silver tables to produce the gold shipment geography output."""
    args = parse_args()
    run_id = args.run_id or create_run_id()
    settings = load_settings()
    ensure_dirs(settings)
    clean_layer_files(settings.gold_layer_dir)
    logger = get_logger("03_gold", settings.log_dir)
    stage_ctx = begin_stage("03_gold", run_id)

    rows_read = 0
    rows_written = 0
    checks_passed = 0
    checks_failed = 0
    try:
        mapping_df = read_mapping_contract()
        target_table = target_table_from_mapping(mapping_df)

        gold_df = _build_shipment_geography_frame(settings.silver_layer_dir)
        rows_written = len(gold_df)

        expected_cols = mapping_df["Target Column Name"].dropna().astype(str).str.strip().tolist()
        expected_cols = [c for c in expected_cols if c]
        if expected_cols:
            missing = [c for c in expected_cols if c not in gold_df.columns]
            passed = len(missing) == 0
            write_quality_check(
                settings.log_dir,
                run_id,
                {
                    "run_id": run_id,
                    "stage": "03_gold",
                    "check_name": "mapped_columns_present",
                    "passed": passed,
                    "details": f"missing={missing}",
                },
            )
            if not passed:
                checks_failed += 1
                raise RuntimeError(f"Gold dataset is missing mapped target columns: {missing}")
            checks_passed += 1
            gold_df = gold_df[expected_cols]

        null_key_count = _check_null_keys(gold_df, "shipment_id")
        duplicate_key_count = _check_duplicate_keys(gold_df, "shipment_id")

        null_check_passed = null_key_count == 0
        write_quality_check(
            settings.log_dir,
            run_id,
            {
                "run_id": run_id,
                "stage": "03_gold",
                "check_name": "shipment_id_not_null",
                "passed": null_check_passed,
                "details": f"null_count={null_key_count}",
            },
        )
        if null_check_passed:
            checks_passed += 1
        else:
            checks_failed += 1
            if settings.strict_mode:
                raise RuntimeError(f"shipment_id has {null_key_count} null values in gold output")

        duplicate_check_passed = duplicate_key_count == 0
        write_quality_check(
            settings.log_dir,
            run_id,
            {
                "run_id": run_id,
                "stage": "03_gold",
                "check_name": "shipment_id_unique",
                "passed": duplicate_check_passed,
                "details": f"duplicate_count={duplicate_key_count}",
            },
        )
        if duplicate_check_passed:
            checks_passed += 1
        else:
            checks_failed += 1
            if settings.strict_mode:
                raise RuntimeError(f"shipment_id has {duplicate_key_count} duplicate values in gold output")

        write_parquet(settings.gold_layer_dir, target_table, gold_df)
        logger.info("run_id=%s | gold.%s built rows=%s", run_id, target_table, len(gold_df))

        metric = end_stage(
            stage_ctx,
            status="success",
            rows_read=rows_read,
            rows_written=rows_written,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
        )
        write_stage_metric(settings.log_dir, run_id, metric)
        logger.info("run_id=%s | Shipment geography gold build completed", run_id)
    except Exception as exc:  # noqa: BLE001
        checks_failed += 1
        metric = end_stage(
            stage_ctx,
            status="failed",
            rows_read=rows_read,
            rows_written=rows_written,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            error_message=str(exc),
        )
        write_stage_metric(settings.log_dir, run_id, metric)
        logger.exception("run_id=%s | Stage failed: %s", run_id, exc)
        raise


if __name__ == "__main__":
    main()
