from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TypeVar

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = Path(__file__).resolve().parents[1]
T = TypeVar("T")


@dataclass(frozen=True)
class Settings:
    bronze_layer_dir: Path
    silver_layer_dir: Path
    gold_layer_dir: Path
    log_dir: Path
    mapping_csv_path: Path
    tidb_host: str
    tidb_port: int
    tidb_user: str
    tidb_password: str
    tidb_database: str
    neon_dsn: str
    neon_host: str
    neon_port: int
    neon_db: str
    neon_user: str
    neon_password: str
    target_schema: str
    batch_size: int
    strict_mode: bool
    max_db_retries: int
    retry_backoff_seconds: float


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().strip('"').strip("'")


def _safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_\-]", "_", name)


def load_settings() -> Settings:
    load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=False)
    return Settings(
        bronze_layer_dir=PIPELINE_ROOT / "parquet" / _env("SG_BRONZE_LAYER_SUBDIR", "bronze"),
        silver_layer_dir=PIPELINE_ROOT / "parquet" / _env("SG_SILVER_LAYER_SUBDIR", "silver"),
        gold_layer_dir=PIPELINE_ROOT / "parquet" / _env("SG_GOLD_LAYER_SUBDIR", "gold"),
        log_dir=PIPELINE_ROOT / _env("SG_LOG_SUBDIR", "logs"),
        mapping_csv_path=PROJECT_ROOT / _env("SG_MAPPING_FILE", "mappings/shipment_geography_mapping.csv"),
        tidb_host=_env("TIDB_HOST", ""),
        tidb_port=int(_env("TIDB_PORT", "4000")),
        tidb_user=_env("TIDB_USER", _env("TIDB_USERNAME", "")),
        tidb_password=_env("TIDB_PASSWORD", ""),
        tidb_database=_env("TIDB_DATABASE", ""),
        neon_dsn=_env("NEON_DSN", ""),
        neon_host=_env("NEON_HOST", ""),
        neon_port=int(_env("NEON_PORT", "5432")),
        neon_db=_env("NEON_DB", ""),
        neon_user=_env("NEON_USER", ""),
        neon_password=_env("NEON_PASSWORD", ""),
        target_schema=_env("TARGET_SCHEMA", "public"),
        batch_size=int(_env("SG_BATCH_SIZE", _env("BATCH_SIZE", "50000"))),
        strict_mode=_truthy(_env("SG_STRICT_MODE", "true")),
        max_db_retries=int(_env("SG_MAX_DB_RETRIES", "3")),
        retry_backoff_seconds=float(_env("SG_RETRY_BACKOFF_SECONDS", "2")),
    )


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def ensure_dirs(settings: Settings) -> None:
    settings.bronze_layer_dir.mkdir(parents=True, exist_ok=True)
    settings.silver_layer_dir.mkdir(parents=True, exist_ok=True)
    settings.gold_layer_dir.mkdir(parents=True, exist_ok=True)
    settings.log_dir.mkdir(parents=True, exist_ok=True)


def get_logger(name: str, log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_dir / f"{name}.log", mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def create_run_id() -> str:
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    token = uuid.uuid4().hex[:8]
    return f"sg_{ts}_{token}"


def begin_stage(stage_name: str, run_id: str) -> dict:
    return {
        "run_id": run_id,
        "stage_name": stage_name,
        "started_at": now_utc_iso(),
        "_started_perf": time.perf_counter(),
    }


def end_stage(
    stage_ctx: dict,
    status: str,
    rows_read: int = 0,
    rows_written: int = 0,
    checks_passed: int = 0,
    checks_failed: int = 0,
    error_message: str = "",
) -> dict:
    started_perf = float(stage_ctx.get("_started_perf", time.perf_counter()))
    duration = round(time.perf_counter() - started_perf, 3)
    return {
        "run_id": stage_ctx["run_id"],
        "stage_name": stage_ctx["stage_name"],
        "status": status,
        "started_at": stage_ctx["started_at"],
        "ended_at": now_utc_iso(),
        "duration_seconds": duration,
        "rows_read": int(rows_read),
        "rows_written": int(rows_written),
        "checks_passed": int(checks_passed),
        "checks_failed": int(checks_failed),
        "error_message": error_message,
    }


def _append_jsonl(file_path: Path, row: dict) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def write_stage_metric(log_dir: Path, run_id: str, metric: dict) -> None:
    _append_jsonl(log_dir / f"{run_id}_stage_metrics.jsonl", metric)


def write_quality_check(log_dir: Path, run_id: str, check_row: dict) -> None:
    _append_jsonl(log_dir / f"{run_id}_data_quality_checks.jsonl", check_row)


def read_jsonl(file_path: Path) -> list[dict]:
    if not file_path.exists():
        return []
    rows: list[dict] = []
    with open(file_path, "r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    return rows


def write_run_summary(log_dir: Path, run_id: str, pipeline_name: str, stage_metrics: list[dict]) -> Path:
    status = "success"
    for metric in stage_metrics:
        if metric.get("status") != "success":
            status = "failed"
            break

    summary = {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "status": status,
        "generated_at": now_utc_iso(),
        "total_stages": len(stage_metrics),
        "rows_read": sum(int(row.get("rows_read", 0)) for row in stage_metrics),
        "rows_written": sum(int(row.get("rows_written", 0)) for row in stage_metrics),
        "checks_passed": sum(int(row.get("checks_passed", 0)) for row in stage_metrics),
        "checks_failed": sum(int(row.get("checks_failed", 0)) for row in stage_metrics),
        "stages": stage_metrics,
    }
    out = log_dir / f"{run_id}_run_summary.json"
    with open(out, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=True, indent=2)
    return out


def run_with_retries(
    action_name: str,
    settings: Settings,
    logger: logging.Logger,
    fn: Callable[[], T],
) -> T:
    last_error: Exception | None = None
    for attempt in range(1, settings.max_db_retries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= settings.max_db_retries:
                break
            delay = settings.retry_backoff_seconds * attempt
            logger.warning(
                "%s failed attempt %s/%s: %s; retrying in %.1fs",
                action_name,
                attempt,
                settings.max_db_retries,
                exc,
                delay,
            )
            time.sleep(delay)
    assert last_error is not None
    raise RuntimeError(f"{action_name} failed after {settings.max_db_retries} attempts: {last_error}") from last_error


def clean_layer_files(layer_dir: Path) -> None:
    if not layer_dir.exists():
        return
    for file in layer_dir.glob("*.parquet"):
        try:
            file.unlink()
        except OSError:
            continue


def parquet_path(layer_dir: Path, table_name: str) -> Path:
    return layer_dir / f"{_safe_name(table_name)}.parquet"


def write_parquet(layer_dir: Path, table_name: str, frame: pd.DataFrame) -> Path:
    layer_dir.mkdir(parents=True, exist_ok=True)
    out = parquet_path(layer_dir, table_name)
    frame.to_parquet(out, index=False)
    return out


def read_parquet(layer_dir: Path, table_name: str) -> pd.DataFrame:
    return pd.read_parquet(parquet_path(layer_dir, table_name))


def list_layer_tables(layer_dir: Path) -> list[str]:
    if not layer_dir.exists():
        return []
    return [p.stem for p in sorted(layer_dir.glob("*.parquet"))]


def read_mapping_contract() -> pd.DataFrame:
    settings = load_settings()
    if not settings.mapping_csv_path.exists():
        raise FileNotFoundError(f"Mapping file not found: {settings.mapping_csv_path}")
    return pd.read_csv(settings.mapping_csv_path)


def source_tables_from_mapping(mapping_df: pd.DataFrame) -> list[str]:
    if "Source Table Name" not in mapping_df.columns:
        raise ValueError("Mapping CSV is missing 'Source Table Name' column")
    values = (
        mapping_df["Source Table Name"]
        .fillna("")
        .astype(str)
        .str.strip()
    )
    tables = sorted({v for v in values if v})
    return tables


def target_table_from_mapping(mapping_df: pd.DataFrame) -> str:
    if "Target Table Name" not in mapping_df.columns:
        raise ValueError("Mapping CSV is missing 'Target Table Name' column")
    values = mapping_df["Target Table Name"].dropna().astype(str).str.strip()
    values = values[values != ""]
    if values.empty:
        raise ValueError("No target table found in mapping CSV")
    return values.iloc[0]


def get_tidb_engine(settings: Settings) -> Engine:
    if not all([settings.tidb_host, settings.tidb_user, settings.tidb_database]):
        raise ValueError("TiDB connection values are missing in .env")
    uri = (
        f"mysql+pymysql://{settings.tidb_user}:{settings.tidb_password}"
        f"@{settings.tidb_host}:{settings.tidb_port}/{settings.tidb_database}?charset=utf8mb4"
    )
    return create_engine(uri, pool_pre_ping=True)


def get_neon_engine(settings: Settings) -> Engine:
    if settings.neon_dsn:
        dsn = settings.neon_dsn
        if dsn.startswith("postgresql://") and "+" not in dsn.split("://", 1)[0]:
            dsn = dsn.replace("postgresql://", "postgresql+psycopg2://", 1)
        return create_engine(dsn, pool_pre_ping=True)

    if all([settings.neon_host, settings.neon_db, settings.neon_user, settings.neon_password]):
        uri = (
            f"postgresql+psycopg2://{settings.neon_user}:{settings.neon_password}"
            f"@{settings.neon_host}:{settings.neon_port}/{settings.neon_db}"
        )
        return create_engine(uri, pool_pre_ping=True)

    raise ValueError("Neon connection values are missing in .env")


def now_utc_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()
