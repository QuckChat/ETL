"""Data ingestion utilities for JDBC/API/File/sample sources."""
"""Data ingestion utilities for JDBC/API/File sources."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit


def ingest_from_jdbc(spark: SparkSession, jdbc_cfg: dict[str, Any]) -> DataFrame:
    """Read source table from a relational DB via JDBC."""

    return (
        spark.read.format("jdbc")
        .option("url", jdbc_cfg["url"])
        .option("dbtable", jdbc_cfg["table"])
        .option("fetchsize", jdbc_cfg.get("fetchsize", 10000))
        .option("user", jdbc_cfg.get("user", ""))
        .option("password", jdbc_cfg.get("password", ""))
        .load()
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_system", lit("rdbms"))
    )


def ingest_from_api_payload(spark: SparkSession, payload: list[dict[str, Any]]) -> DataFrame:
    """Create DataFrame from API response payload."""

    rows = [json.dumps(row) for row in payload]
    df = spark.read.json(spark.sparkContext.parallelize(rows))
    return df.withColumn("ingestion_ts", current_timestamp()).withColumn(
        "source_system", lit("api")
    )


def ingest_from_files(spark: SparkSession, path: str, fmt: str = "csv") -> DataFrame:
    """Read files dropped in data lake into bronze DataFrame."""

    reader = spark.read
    if fmt == "csv":
        reader = reader.option("header", True).option("inferSchema", True)

    return reader.format(fmt).load(path).withColumn("ingestion_ts", current_timestamp())


def ingest_sample_orders(spark: SparkSession, batch_date: str | None = None) -> DataFrame:
    """Build deterministic sample dataset for local development/demo."""

    order_ts = batch_date or datetime.utcnow().strftime("%Y-%m-%d")
    rows = [
        {"order_id": "O-1", "customer_id": "C-10", "order_ts": f"{order_ts} 01:00:00", "amount": 120.4, "status": "COMPLETE"},
        {"order_id": "O-2", "customer_id": "C-20", "order_ts": f"{order_ts} 02:10:00", "amount": 35.9, "status": "COMPLETE"},
        {"order_id": "O-3", "customer_id": "C-10", "order_ts": f"{order_ts} 08:32:00", "amount": 12.0, "status": "PENDING"},
    ]
    return ingest_from_api_payload(spark, rows).withColumn("source_system", lit("sample"))
