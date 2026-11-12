"""Runtime settings, resolved from environment variables.

Everything that differs between "inside docker compose", "on a laptop" and
"in CI" is an environment variable with a sensible default here, so the rest of
the code never reads ``os.environ`` directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def project_root() -> Path:
    env = os.environ.get("OPENLAKE_HOME")
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str = field(default_factory=lambda: _env("OPENLAKE_S3_ENDPOINT", "http://localhost:9000"))
    access_key: str = field(default_factory=lambda: _env("AWS_ACCESS_KEY_ID", "openlake"))
    secret_key: str = field(default_factory=lambda: _env("AWS_SECRET_ACCESS_KEY", "openlake-secret"))
    region: str = field(default_factory=lambda: _env("AWS_REGION", "us-east-1"))
    bucket: str = field(default_factory=lambda: _env("OPENLAKE_BUCKET", "openlake"))

    @property
    def host_port(self) -> str:
        return self.endpoint.split("://", 1)[-1].rstrip("/")

    @property
    def use_ssl(self) -> bool:
        return self.endpoint.startswith("https://")

    def s3_uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key.lstrip('/')}"

    def s3a_uri(self, key: str) -> str:
        return f"s3a://{self.bucket}/{key.lstrip('/')}"


@dataclass(frozen=True)
class CatalogSettings:
    name: str = field(default_factory=lambda: _env("OPENLAKE_CATALOG", "lake"))
    uri: str = field(default_factory=lambda: _env("OPENLAKE_CATALOG_URI", "http://localhost:8181"))
    # "rest" in the platform; "hadoop" (filesystem catalog) for hermetic unit tests.
    type: str = field(default_factory=lambda: _env("OPENLAKE_CATALOG_TYPE", "rest"))
    warehouse: str = field(default_factory=lambda: _env("OPENLAKE_WAREHOUSE", ""))


@dataclass(frozen=True)
class SparkSettings:
    master: str = field(default_factory=lambda: _env("OPENLAKE_SPARK_MASTER", "local[*]"))
    driver_memory: str = field(default_factory=lambda: _env("OPENLAKE_DRIVER_MEMORY", "2g"))
    executor_memory: str = field(default_factory=lambda: _env("OPENLAKE_EXECUTOR_MEMORY", "2g"))
    executor_cores: str = field(default_factory=lambda: _env("OPENLAKE_EXECUTOR_CORES", "2"))
    shuffle_partitions: str = field(default_factory=lambda: _env("OPENLAKE_SHUFFLE_PARTITIONS", "8"))
    # Optional Maven coordinates, used when Iceberg jars are not on the classpath (CI).
    packages: str = field(default_factory=lambda: _env("OPENLAKE_SPARK_PACKAGES", ""))
    checkpoint_dir: str = field(default_factory=lambda: _env("OPENLAKE_CHECKPOINT_DIR", str(project_root() / "checkpoints")))


@dataclass(frozen=True)
class Settings:
    minio: MinioSettings = field(default_factory=MinioSettings)
    catalog: CatalogSettings = field(default_factory=CatalogSettings)
    spark: SparkSettings = field(default_factory=SparkSettings)
    data_dir: Path = field(default_factory=lambda: Path(_env("OPENLAKE_DATA_DIR", str(project_root() / "data"))))
    reports_dir: Path = field(default_factory=lambda: Path(_env("OPENLAKE_REPORTS_DIR", str(project_root() / "reports"))))
    config_path: Path = field(
        default_factory=lambda: Path(_env("OPENLAKE_CONFIG", str(project_root() / "config" / "pipeline_config.yaml")))
    )
    scale_orders: int = field(default_factory=lambda: int(_env("OPENLAKE_SCALE_ORDERS", "100000")))


def get_settings() -> Settings:
    return Settings()
