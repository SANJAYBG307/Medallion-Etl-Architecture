from __future__ import annotations

import argparse
import pandas as pd
from sqlalchemy import text

from sg_common import (
    begin_stage,
    clean_layer_files,
    create_run_id,
    end_stage,
    ensure_dirs,
    get_logger,
    get_tidb_engine,
    load_settings,
    read_mapping_contract,
    run_with_retries,
    source_tables_from_mapping,
    write_quality_check,
    write_stage_metric,
    write_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import shipment geography source tables directly into bronze layer.")
    parser.add_argument("--run-id", default="", help="Optional run id shared across all pipeline stages.")
    return parser.parse_args()


def main() -> None:
    """Stage 1: read source tables from TiDB and write bronze parquet files."""
    args = parse_args()
    run_id = args.run_id or create_run_id()
    settings = load_settings()
    ensure_dirs(settings)
    clean_layer_files(settings.bronze_layer_dir)
    logger = get_logger("01_import_to_bronze", settings.log_dir)
    stage_ctx = begin_stage("01_import_to_bronze", run_id)

    rows_read = 0
    rows_written = 0
    checks_passed = 0
    checks_failed = 0
    try:
        mapping_df = read_mapping_contract()
        source_tables = source_tables_from_mapping(mapping_df)
        if not source_tables:
            raise RuntimeError("No source tables found in mapping CSV")

        write_quality_check(
            settings.log_dir,
            run_id,
            {
                "run_id": run_id,
                "stage": "01_import_to_bronze",
                "check_name": "source_tables_present",
                "passed": True,
                "details": f"table_count={len(source_tables)}",
            },
        )
        checks_passed += 1

        engine = get_tidb_engine(settings)
        with engine.connect() as conn:
            for table in source_tables:
                # Retry source reads so temporary network issues do not fail the full run.
                def _read_table() -> pd.DataFrame:
                    query = text(f"SELECT * FROM `{table}`")
                    return pd.read_sql(query, conn)

                df = run_with_retries(f"read source table {table}", settings, logger, _read_table)
                write_parquet(settings.bronze_layer_dir, table, df)

                table_rows = len(df)
                rows_read += table_rows
                rows_written += table_rows
                logger.info("run_id=%s | bronze.%s loaded rows=%s", run_id, table, table_rows)

                write_quality_check(
                    settings.log_dir,
                    run_id,
                    {
                        "run_id": run_id,
                        "stage": "01_import_to_bronze",
                        "check_name": f"table_loaded_{table}",
                        "passed": True,
                        "details": f"rows={table_rows}",
                    },
                )
                checks_passed += 1

        metric = end_stage(
            stage_ctx,
            status="success",
            rows_read=rows_read,
            rows_written=rows_written,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
        )
        write_stage_metric(settings.log_dir, run_id, metric)
        logger.info("run_id=%s | Shipment geography import to bronze completed", run_id)
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
