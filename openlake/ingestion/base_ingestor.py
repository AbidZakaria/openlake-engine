"""Format adapters for the ingestion service.

An ingestor knows how to *inspect* a raw file of one format: check that it is
structurally readable, count its records and list its columns. It never
changes the file — Bronze preserves source files byte-for-byte.
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path


class IngestionError(Exception):
    """A file cannot be ingested (unreadable, empty, malformed)."""


@dataclass
class FileProfile:
    path: Path
    format: str
    size_bytes: int
    sha256: str
    record_count: int | None
    columns: list[str] = field(default_factory=list)


def sha256_of(path: Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(chunk):
            digest.update(block)
    return digest.hexdigest()


class BaseIngestor(ABC):
    format: str = ""
    extensions: tuple[str, ...] = ()

    def accepts(self, path: Path) -> bool:
        return path.suffix.lower() in self.extensions

    def profile(self, path: Path) -> FileProfile:
        path = Path(path)
        if not path.is_file():
            raise IngestionError(f"{path} is not a readable file")
        size = path.stat().st_size
        if size == 0:
            raise IngestionError(f"{path.name} is empty")
        record_count, columns = self.inspect(path)
        if not columns:
            raise IngestionError(f"{path.name}: no columns detected")
        if len(set(columns)) != len(columns):
            raise IngestionError(f"{path.name}: duplicate column names {columns}")
        return FileProfile(path, self.format, size, sha256_of(path), record_count, columns)

    @abstractmethod
    def inspect(self, path: Path) -> tuple[int | None, list[str]]:
        """Return (record_count, column_names) or raise IngestionError."""
