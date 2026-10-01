from __future__ import annotations

import argparse
from sqlalchemy import inspect, text

from sg_common import (
    begin_stage,
    create_run_id,
    end_stage,
    ensure_dirs,
    get_logger,
    get_neon_engine,
    load_settings,
    read_mapping_contract,
    read_parquet,
    run_with_retries,
    target_table_from_mapping,
    write_quality_check,
    write_stage_metric,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load shipment geography gold data into Neon target table.")
    parser.add_argument("--run-id", default="", help="Optional run id shared across all pipeline stages.")
    return parser.parse_args()


def main() -> None:
    """Stage 4: validate target table and load gold parquet into Neon."""
    args = parse_args()
    run_id = args.run_id or create_run_id()
    settings = load_settings()
    ensure_dirs(settings)
    logger = get_logger("04_load_neon", settings.log_dir)
    stage_ctx = begin_stage("04_load_neon", run_id)

    rows_read = 0
    rows_written = 0
    checks_passed = 0
    checks_failed = 0
    try:
        mapping_df = read_mapping_contract()
        target_table = target_table_from_mapping(mapping_df)

        gold_df = read_parquet(settings.gold_layer_dir, target_table)
        rows_read = len(gold_df)
        engine = get_neon_engine(settings)

        inspector = inspect(engine)
        table_exists = inspector.has_table(target_table, schema=settings.target_schema)
        write_quality_check(
            settings.log_dir,
            run_id,
            {
                "run_id": run_id,
                "stage": "04_load_neon",
                "check_name": "target_table_exists",
                "passed": table_exists,
                "details": f"schema={settings.target_schema}, table={target_table}",
            },
        )
        if not table_exists:
            checks_failed += 1
            raise RuntimeError(f"Target table {settings.target_schema}.{target_table} not found in Neon")
        checks_passed += 1

        with engine.begin() as conn:
            pre_count = conn.execute(
                text(f'SELECT COUNT(*) FROM "{settings.target_schema}"."{target_table}"')
            ).scalar_one()
            logger.info("run_id=%s | preload existing rows in %s = %s", run_id, target_table, pre_count)

            if gold_df.empty:
                logger.info("run_id=%s | preserving %s because gold has no rows", run_id, target_table)
            else:
                conn.execute(text(f'TRUNCATE TABLE "{settings.target_schema}"."{target_table}"'))
                logger.info("run_id=%s | truncated %s.%s", run_id, settings.target_schema, target_table)

                # to_sql uses the same active transaction connection.
                run_with_retries(
                    f"load {target_table} to Neon",
                    settings,
                    logger,
                    lambda: gold_df.to_sql(
                        name=target_table,
                        con=conn,
                        schema=settings.target_schema,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=settings.batch_size,
                    ),
                )
                rows_written = len(gold_df)
                logger.info("run_id=%s | loaded rows into %s = %s", run_id, target_table, rows_written)

            post_count = conn.execute(
                text(f'SELECT COUNT(*) FROM "{settings.target_schema}"."{target_table}"')
            ).scalar_one()
            logger.info("run_id=%s | postload rows in %s = %s", run_id, target_table, post_count)

            load_count_check = gold_df.empty or post_count == len(gold_df)
            write_quality_check(
                settings.log_dir,
                run_id,
                {
                    "run_id": run_id,
                    "stage": "04_load_neon",
                    "check_name": "postload_count_check",
                    "passed": load_count_check,
                    "details": f"gold_rows={len(gold_df)}, postload_rows={post_count}",
                },
            )
            if load_count_check:
                checks_passed += 1
            else:
                checks_failed += 1
                if settings.strict_mode:
                    raise RuntimeError(f"Postload count mismatch: gold={len(gold_df)} postload={post_count}")

        metric = end_stage(
            stage_ctx,
            status="success",
            rows_read=rows_read,
            rows_written=rows_written,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
        )
        write_stage_metric(settings.log_dir, run_id, metric)
        logger.info("run_id=%s | Shipment geography Neon load completed", run_id)
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
