from __future__ import annotations

from pathlib import Path

from openlake.ingestion.base_ingestor import BaseIngestor, IngestionError


class ParquetIngestor(BaseIngestor):
    format = "parquet"
    extensions = (".parquet", ".pq")

    def inspect(self, path: Path) -> tuple[int | None, list[str]]:
        import pyarrow.parquet as pq

        try:
            meta = pq.ParquetFile(path)
        except Exception as exc:  # pyarrow raises several exception types
            raise IngestionError(f"{path.name}: unreadable Parquet ({exc})") from exc
        return meta.metadata.num_rows, list(meta.schema_arrow.names)
