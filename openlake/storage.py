"""Object storage abstraction.

The ingestion layer only needs a handful of operations (put, get, list, head).
Hiding them behind a tiny interface lets the same code run against MinIO in
the platform and against a temporary directory in unit tests.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from openlake.config.settings import MinioSettings


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int


class ObjectStore(Protocol):
    bucket: str

    def put_file(self, local_path: Path, key: str) -> None: ...
    def put_bytes(self, data: bytes, key: str, content_type: str = "application/octet-stream") -> None: ...
    def get_bytes(self, key: str) -> bytes: ...
    def exists(self, key: str) -> bool: ...
    def list(self, prefix: str) -> Iterator[ObjectInfo]: ...
    def delete(self, key: str) -> None: ...

    def uri(self, key: str) -> str: ...
    def spark_uri(self, key: str) -> str: ...


def put_json(store: ObjectStore, key: str, payload: dict) -> None:
    store.put_bytes(json.dumps(payload, indent=2, default=str).encode(), key, "application/json")


def get_json(store: ObjectStore, key: str) -> dict:
    return json.loads(store.get_bytes(key))


class S3ObjectStore:
    """MinIO / S3 implementation backed by boto3."""

    def __init__(self, settings: MinioSettings):
        import boto3
        from botocore.config import Config

        self.settings = settings
        self.bucket = settings.bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.endpoint,
            aws_access_key_id=settings.access_key,
            aws_secret_access_key=settings.secret_key,
            region_name=settings.region,
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
        )

    def ensure_bucket(self) -> bool:
        """Create the bucket if needed. Returns True when it was created."""
        from botocore.exceptions import ClientError

        try:
            self.client.head_bucket(Bucket=self.bucket)
            return False
        except ClientError:
            self.client.create_bucket(Bucket=self.bucket)
            return True

    def put_file(self, local_path: Path, key: str) -> None:
        self.client.upload_file(str(local_path), self.bucket, key)

    def put_bytes(self, data: bytes, key: str, content_type: str = "application/octet-stream") -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def get_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield ObjectInfo(obj["Key"], obj["Size"])

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def spark_uri(self, key: str) -> str:
        """URI Spark's Hadoop layer reads raw files through (S3A)."""
        return f"s3a://{self.bucket}/{key}"


class LocalObjectStore:
    """Directory-backed implementation for tests and offline experiments."""

    def __init__(self, root: Path, bucket: str = "openlake"):
        self.bucket = bucket
        self.root = Path(root) / bucket
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key

    def put_file(self, local_path: Path, key: str) -> None:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, target)

    def put_bytes(self, data: bytes, key: str, content_type: str = "application/octet-stream") -> None:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    def get_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        for path in sorted(self.root.rglob("*")):
            if path.is_file():
                key = path.relative_to(self.root).as_posix()
                if key.startswith(prefix):
                    yield ObjectInfo(key, path.stat().st_size)

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)

    def uri(self, key: str) -> str:
        return self._path(key).as_uri()

    def spark_uri(self, key: str) -> str:
        # Not Path.as_uri(): it percent-encodes "=" in Hive-style paths, which Spark reads literally.
        return "file://" + self._path(key).resolve().as_posix()


def default_store() -> S3ObjectStore:
    from openlake.config.settings import get_settings

    return S3ObjectStore(get_settings().minio)
