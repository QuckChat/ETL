import pytest

pytest.importorskip("yaml")

from pipeline.config import load_config


def test_load_config_reads_expected_sections() -> None:
    cfg = load_config("conf/pipeline.yml")

    assert cfg.app_name == "cdp_orders_etl"
    assert "silver_orders" in cfg.paths
    assert cfg.quality["primary_key"] == "order_id"
