"""SparkSession factory wired to the Iceberg catalog and MinIO.

Two catalog modes are supported:

* ``rest``   — the platform: Iceberg REST catalog + S3FileIO against MinIO.
* ``hadoop`` — a filesystem catalog in a local directory, used by the hermetic
  Spark unit tests so they need neither MinIO nor the catalog service.
"""

from __future__ import annotations

import socket

from openlake.config.settings import Settings, get_settings

ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"


def _driver_host() -> str:
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "127.0.0.1"


def spark_conf(settings: Settings, app_name: str) -> dict[str, str]:
    cat = settings.catalog
    s3 = settings.minio
    prefix = f"spark.sql.catalog.{cat.name}"
    conf: dict[str, str] = {
        "spark.app.name": app_name,
        "spark.master": settings.spark.master,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": cat.name,
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.shuffle.partitions": settings.spark.shuffle_partitions,
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
        "spark.driver.memory": settings.spark.driver_memory,
        "spark.executor.memory": settings.spark.executor_memory,
        "spark.executor.cores": settings.spark.executor_cores,
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.iceberg.vectorization.enabled": "true",
    }
    if cat.type == "rest":
        conf.update(
            {
                f"{prefix}.type": "rest",
                f"{prefix}.uri": cat.uri,
                f"{prefix}.warehouse": cat.warehouse or s3.s3_uri("warehouse"),
                f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                f"{prefix}.s3.endpoint": s3.endpoint,
                f"{prefix}.s3.path-style-access": "true",
                f"{prefix}.s3.access-key-id": s3.access_key,
                f"{prefix}.s3.secret-access-key": s3.secret_key,
                f"{prefix}.client.region": s3.region,
                # Plain S3A access for the raw (non-Iceberg) landing zone.
                "spark.hadoop.fs.s3a.endpoint": s3.endpoint,
                "spark.hadoop.fs.s3a.access.key": s3.access_key,
                "spark.hadoop.fs.s3a.secret.key": s3.secret_key,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": str(s3.use_ssl).lower(),
                "spark.hadoop.fs.s3a.endpoint.region": s3.region,
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                # Lets Hadoop-based actions (orphan-file listing) resolve the s3:// paths Iceberg writes.
                "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            }
        )
    else:
        conf.update({f"{prefix}.type": "hadoop", f"{prefix}.warehouse": cat.warehouse})
    if settings.spark.master.startswith("spark://"):
        conf["spark.driver.host"] = _driver_host()
        conf["spark.driver.bindAddress"] = "0.0.0.0"
    if settings.spark.packages:
        conf["spark.jars.packages"] = settings.spark.packages
    return conf


def build_spark(app_name: str = "openlake", settings: Settings | None = None, extra: dict[str, str] | None = None):
    from pyspark.sql import SparkSession

    settings = settings or get_settings()
    builder = SparkSession.builder
    for key, value in {**spark_conf(settings, app_name), **(extra or {})}.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
