"""Transformation and enrichment logic for ETL datasets."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, round as spark_round, to_timestamp


def transform_orders(raw_orders: DataFrame) -> DataFrame:
    """Cleanse and standardize orders data into silver format."""

    return (
        raw_orders.select(
            col("order_id").cast("string"),
            col("customer_id").cast("string"),
            to_timestamp(col("order_ts")).alias("order_ts"),
            spark_round(col("amount").cast("double"), 2).alias("amount"),
            col("status").cast("string"),
            col("source_system"),
            col("ingestion_ts"),
        )
        .dropna(subset=["order_id", "customer_id", "order_ts"])
        .dropDuplicates(["order_id"])
        .withColumn("order_date", date_format("order_ts", "yyyy-MM-dd"))
    )


def build_daily_sales(silver_orders: DataFrame) -> DataFrame:
    """Aggregate silver orders into gold daily KPI dataset."""

    return silver_orders.groupBy("order_date").sum("amount").withColumnRenamed(
        "sum(amount)", "daily_revenue"
    )
