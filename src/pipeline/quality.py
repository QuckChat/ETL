"""Data quality validations for ETL pipeline."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count


@dataclass
class QualityResult:
    """Quality check output used for gating downstream writes."""

    passed: bool
    metrics: dict[str, int]


def check_required_columns(df: DataFrame, required_columns: list[str]) -> bool:
    """Ensure all expected columns are present."""

    return all(column in df.columns for column in required_columns)


def check_duplicate_keys(df: DataFrame, primary_key: str) -> int:
    """Return number of duplicated key groups."""

    return (
        df.groupBy(primary_key)
        .agg(count("*").alias("cnt"))
        .filter(col("cnt") > 1)
        .count()
    )


def check_negative_amounts(df: DataFrame, amount_column: str, minimum: float) -> int:
    """Return count of rows violating minimum threshold."""

    return df.filter(col(amount_column) < minimum).count()


def run_quality_checks(
    df: DataFrame,
    required_columns: list[str],
    primary_key: str,
    amount_column: str = "amount",
    amount_min: float = 0,
    min_rows: int = 1,
) -> QualityResult:
    """Execute quality checks and summarize results."""

    total_rows = df.count()
    has_required_columns = check_required_columns(df, required_columns)
    duplicate_groups = check_duplicate_keys(df, primary_key)
    invalid_amount_rows = check_negative_amounts(df, amount_column, amount_min)

    metrics = {
        "total_rows": total_rows,
        "missing_required_columns": 0 if has_required_columns else 1,
        "duplicate_key_groups": duplicate_groups,
        "invalid_amount_rows": invalid_amount_rows,
        "below_min_rows": 1 if total_rows < min_rows else 0,
    }

    passed = (
        has_required_columns
        and duplicate_groups == 0
        and invalid_amount_rows == 0
        and total_rows >= min_rows
    )
    return QualityResult(passed=passed, metrics=metrics)
