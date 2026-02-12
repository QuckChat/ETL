from pyspark.sql import SparkSession

from pipeline.quality import run_quality_checks


def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName("quality-tests")
        .getOrCreate()
    )


def test_quality_passes_for_valid_rows() -> None:
    session = spark()
    df = session.createDataFrame(
        [
            {"order_id": "1", "customer_id": "A", "order_ts": "2025-01-01 01:00:00", "amount": 10.0},
            {"order_id": "2", "customer_id": "B", "order_ts": "2025-01-01 02:00:00", "amount": 20.0},
        ]
    )

    result = run_quality_checks(df, ["order_id", "customer_id", "order_ts", "amount"], "order_id")

    assert result.passed
    assert result.metrics["duplicate_key_groups"] == 0
    session.stop()


def test_quality_fails_for_negative_amounts() -> None:
    session = spark()
    df = session.createDataFrame(
        [{"order_id": "1", "customer_id": "A", "order_ts": "2025-01-01 01:00:00", "amount": -10.0}]
    )

    result = run_quality_checks(df, ["order_id", "customer_id", "order_ts", "amount"], "order_id")

    assert not result.passed
    assert result.metrics["invalid_amount_rows"] == 1
    session.stop()
