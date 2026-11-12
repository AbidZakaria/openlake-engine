from __future__ import annotations

import json
from pathlib import Path

from openlake.ingestion.base_ingestor import BaseIngestor, IngestionError


class JsonIngestor(BaseIngestor):
    """Newline-delimited JSON (one object per line), the format Spark reads natively."""

    format = "json"
    extensions = (".json", ".jsonl", ".ndjson")

    def inspect(self, path: Path) -> tuple[int | None, list[str]]:
        columns: dict[str, None] = {}
        count = 0
        bad = 0
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                count += 1
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    bad += 1
                    continue
                if not isinstance(obj, dict):
                    bad += 1
                    continue
                for key in obj:
                    columns.setdefault(key, None)
        if count == 0:
            raise IngestionError(f"{path.name}: no JSON records")
        if bad / count > 0.5:
            raise IngestionError(f"{path.name}: {bad}/{count} lines are not JSON objects")
        return count, list(columns)
