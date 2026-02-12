"""Load utilities for writing silver and gold outputs."""

from __future__ import annotations

from pyspark.sql import DataFrame


def write_partitioned_parquet(df: DataFrame, output_path: str, partition_column: str) -> None:
    """Persist DataFrame in parquet with partitioning strategy."""

    (
        df.write.mode("overwrite")
        .partitionBy(partition_column)
        .format("parquet")
        .save(output_path)
    )


def write_hive_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """Write DataFrame to Hive managed table."""

    df.write.mode(mode).format("hive").saveAsTable(table_name)
