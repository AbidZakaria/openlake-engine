from __future__ import annotations

import csv
from pathlib import Path

from openlake.ingestion.base_ingestor import BaseIngestor, IngestionError


class CsvIngestor(BaseIngestor):
    format = "csv"
    extensions = (".csv",)

    def inspect(self, path: Path) -> tuple[int | None, list[str]]:
        try:
            with open(path, newline="", encoding="utf-8") as fh:
                reader = csv.reader(fh)
                header = next(reader, None)
                if not header:
                    raise IngestionError(f"{path.name}: missing CSV header")
                columns = [h.strip() for h in header]
                width = len(columns)
                count = 0
                ragged = 0
                for row in reader:
                    if not row:
                        continue
                    count += 1
                    if len(row) != width:
                        ragged += 1
        except UnicodeDecodeError as exc:
            raise IngestionError(f"{path.name}: not UTF-8 encoded ({exc})") from exc
        # A handful of ragged rows is a data-quality problem for Silver; a file
        # that is mostly ragged is structurally broken and rejected here.
        if count and ragged / count > 0.5:
            raise IngestionError(f"{path.name}: {ragged}/{count} rows do not match the header width")
        return count, columns
