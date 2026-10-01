from __future__ import annotations

import argparse
import pandas as pd

from sg_common import (
    begin_stage,
    clean_layer_files,
    create_run_id,
    end_stage,
    ensure_dirs,
    get_logger,
    list_layer_tables,
    load_settings,
    read_parquet,
    write_quality_check,
    write_stage_metric,
    write_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean bronze data and build silver layer.")
    parser.add_argument("--run-id", default="", help="Optional run id shared across all pipeline stages.")
    return parser.parse_args()


def _clean_frame(df: pd.DataFrame) -> pd.DataFrame:
    # Keep cleaning simple for beginners: trim spaces and treat empty strings as null.
    out = df.copy()
    object_cols = out.select_dtypes(include=["object", "string"]).columns
    for col in object_cols:
        out[col] = out[col].apply(lambda v: v.strip() if isinstance(v, str) else v)
        out[col] = out[col].replace("", pd.NA)
    return out


def main() -> None:
    """Stage 2: convert bronze parquet files into cleaned silver parquet files."""
    args = parse_args()
    run_id = args.run_id or create_run_id()
    settings = load_settings()
    ensure_dirs(settings)
    clean_layer_files(settings.silver_layer_dir)
    logger = get_logger("02_silver", settings.log_dir)
    stage_ctx = begin_stage("02_silver", run_id)

    rows_read = 0
    rows_written = 0
    checks_passed = 0
    checks_failed = 0
    try:
        src_tables = list_layer_tables(settings.bronze_layer_dir)
        if not src_tables:
            raise RuntimeError("No bronze parquet files found. Run 01_import_to_bronze.py first.")

        for table in src_tables:
            source_df = read_parquet(settings.bronze_layer_dir, table)
            silver_df = _clean_frame(source_df)
            write_parquet(settings.silver_layer_dir, table, silver_df)

            source_count = len(source_df)
            target_count = len(silver_df)
            rows_read += source_count
            rows_written += target_count

            passed = source_count == target_count
            write_quality_check(
                settings.log_dir,
                run_id,
                {
                    "run_id": run_id,
                    "stage": "02_silver",
                    "check_name": f"row_count_match_{table}",
                    "passed": passed,
                    "details": f"source={source_count}, target={target_count}",
                },
            )

            if not passed:
                checks_failed += 1
                raise RuntimeError(f"Row count mismatch for {table}: source={source_count} target={target_count}")

            checks_passed += 1
            logger.info("run_id=%s | silver.%s ready rows=%s", run_id, table, target_count)

        metric = end_stage(
            stage_ctx,
            status="success",
            rows_read=rows_read,
            rows_written=rows_written,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
        )
        write_stage_metric(settings.log_dir, run_id, metric)
        logger.info("run_id=%s | Shipment geography silver build completed", run_id)
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
