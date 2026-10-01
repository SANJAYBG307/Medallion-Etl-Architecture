# Shipment Geography ETL Pipeline

This pipeline follows the same layered approach as the existing module, with these design choices:

- Uses Pandas instead of Polars
- Imports source data directly into the Bronze layer (no Raw layer)
- Uses existing credentials from the root .env file
- Includes enterprise-style logging, monitoring, and quality checks

## Stage Execution Order

1. scripts/01_import_to_bronze.py
2. scripts/02_silver.py
3. scripts/03_gold.py
4. scripts/04_load_neon.py

Automation runner:

- scripts/run_pipeline.py

## Folder Structure

- parquet/bronze
- parquet/silver
- parquet/gold
- logs

## Mapping Contract

- mappings/shipment_geography_mapping.csv

## Environment

Credentials and target schema are read from the root .env file.

Optional pipeline-specific keys (all have defaults):

- SG_MAPPING_FILE (default: mappings/shipment_geography_mapping.csv)
- SG_BRONZE_LAYER_SUBDIR (default: bronze)
- SG_SILVER_LAYER_SUBDIR (default: silver)
- SG_GOLD_LAYER_SUBDIR (default: gold)
- SG_LOG_SUBDIR (default: logs)
- SG_BATCH_SIZE (default: BATCH_SIZE or 50000)
- SG_STRICT_MODE (default: true)
- SG_MAX_DB_RETRIES (default: 3)
- SG_RETRY_BACKOFF_SECONDS (default: 2)

## Run Commands (using existing myenv)

From project root:

- .\\myenv\\Scripts\\python.exe .\\Shipment_Geography_ETL_Pipeline\\scripts\\01_import_to_bronze.py
- .\\myenv\\Scripts\\python.exe .\\Shipment_Geography_ETL_Pipeline\\scripts\\02_silver.py
- .\\myenv\\Scripts\\python.exe .\\Shipment_Geography_ETL_Pipeline\\scripts\\03_gold.py
- .\\myenv\\Scripts\\python.exe .\\Shipment_Geography_ETL_Pipeline\\scripts\\04_load_neon.py

Single command full run:

- .\\myenv\\Scripts\\python.exe .\\Shipment_Geography_ETL_Pipeline\\scripts\\run_pipeline.py

Partial run example (from silver to load):

- .\\myenv\\Scripts\\python.exe .\\Shipment_Geography_ETL_Pipeline\\scripts\\run_pipeline.py --from-stage 02 --to-stage 04

## Monitoring Outputs

For each run_id, the logs folder stores:

- {run_id}_stage_metrics.jsonl
- {run_id}_data_quality_checks.jsonl
- {run_id}_run_summary.json

These files can be used for dashboards, alerting, and audit reporting.

## Data Quality Gates

- Source tables must be present in mapping
- Bronze to silver row count must match
- Gold mapped columns must exist
- shipment_id null and duplicate checks in gold
- Target table existence check before load
- Post-load row count check
