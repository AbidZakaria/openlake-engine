"""Shared fixtures.

Three tiers of tests:

* unit (default)        — pure Python, no JVM, no services
* ``-m spark``          — local Spark + Iceberg *filesystem* catalog in a temp dir;
                          runs in the openlake image or in CI with pyspark installed
* ``-m integration``    — against the running ``docker compose`` stack
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def config():
    from openlake.config.pipeline import load_config

    return load_config(ROOT / "config" / "pipeline_config.yaml")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    warehouse = tmp_path_factory.mktemp("warehouse")
    os.environ.update({
        "OPENLAKE_CATALOG_TYPE": "hadoop",
        "OPENLAKE_WAREHOUSE": warehouse.as_uri(),
        "OPENLAKE_SPARK_MASTER": "local[2]",
        "OPENLAKE_SHUFFLE_PARTITIONS": "2",
        "OPENLAKE_DRIVER_MEMORY": "1g",
    })
    from openlake.config.settings import Settings
    from openlake.config.spark import build_spark

    session = build_spark("openlake-tests", Settings(), extra={"spark.ui.enabled": "false",
                                                              "spark.sql.adaptive.enabled": "false"})
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def local_store(tmp_path):
    from openlake.storage import LocalObjectStore

    return LocalObjectStore(tmp_path / "objects")


@pytest.fixture(scope="session")
def small_dataset(tmp_path_factory):
    """A tiny generated dataset: historical batch + two CDC batches."""
    from openlake.datagen.cdc_simulator import simulate_batch
    from openlake.datagen.generator import generate_initial

    root = tmp_path_factory.mktemp("generated")
    generate_initial(root, n_orders=600, seed=11)
    simulate_batch(root, 2, new_orders=40)
    simulate_batch(root, 3, new_orders=40)
    return root
