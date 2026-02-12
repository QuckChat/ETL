"""Main ETL entrypoint orchestrating ingestion, transform, quality, and load."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from pipeline.config import load_config
from pipeline.ingestion import ingest_from_jdbc, ingest_sample_orders
from pipeline.ingestion import ingest_from_jdbc
from pipeline.load import write_partitioned_parquet
from pipeline.quality import run_quality_checks
from pipeline.transformation import build_daily_sales, transform_orders


def build_spark(app_name: str) -> SparkSession:
    """Create Spark session with Hive support."""

    return SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()


def run(config_path: str, source: str = "jdbc") -> None:
def run(config_path: str) -> None:
    """Execute end-to-end ETL run."""

    cfg = load_config(config_path)
    spark = build_spark(cfg.app_name)

    try:
        if source == "sample":
            raw_orders = ingest_sample_orders(spark, cfg.batch_date)
        else:
            raw_orders = ingest_from_jdbc(spark, cfg.jdbc)

        silver_orders = transform_orders(raw_orders)

        quality_result = run_quality_checks(
            silver_orders,
            required_columns=cfg.quality["required_columns"],
            primary_key=cfg.quality["primary_key"],
            amount_min=cfg.quality.get("amount_min", 0),
            min_rows=cfg.quality.get("min_rows", 1),
        )

        if not quality_result.passed:
            quarantine_path = cfg.paths.get("quarantine_orders")
            if quarantine_path:
                write_partitioned_parquet(silver_orders, quarantine_path, "order_date")
            raise ValueError(f"Quality checks failed: {quality_result.metrics}")

        daily_sales = build_daily_sales(silver_orders)

        write_partitioned_parquet(silver_orders, cfg.paths["silver_orders"], "order_date")
        write_partitioned_parquet(daily_sales, cfg.paths["gold_daily_sales"], "order_date")
    finally:
        spark.stop()
    raw_orders = ingest_from_jdbc(spark, cfg.jdbc)
    silver_orders = transform_orders(raw_orders)

    quality_result = run_quality_checks(
        silver_orders,
        required_columns=cfg.quality["required_columns"],
        primary_key=cfg.quality["primary_key"],
        amount_min=cfg.quality.get("amount_min", 0),
    )

    if not quality_result.passed:
        raise ValueError(f"Quality checks failed: {quality_result.metrics}")

    daily_sales = build_daily_sales(silver_orders)

    write_partitioned_parquet(silver_orders, cfg.paths["silver_orders"], "order_date")
    write_partitioned_parquet(daily_sales, cfg.paths["gold_daily_sales"], "order_date")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run CDP ETL pipeline")
    parser.add_argument("--config", default="conf/pipeline.yml", help="Path to config file")
    parser.add_argument(
        "--source",
        choices=["jdbc", "sample"],
        default="jdbc",
        help="Select ingestion source. Use 'sample' for local validation without external systems.",
    )
    args = parser.parse_args()

    run(args.config, args.source)
    args = parser.parse_args()

    run(args.config)
