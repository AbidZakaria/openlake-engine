from openlake.ingestion.base_ingestor import BaseIngestor, FileProfile, IngestionError
from openlake.ingestion.csv_ingestor import CsvIngestor
from openlake.ingestion.json_ingestor import JsonIngestor
from openlake.ingestion.parquet_ingestor import ParquetIngestor

INGESTORS: tuple[BaseIngestor, ...] = (CsvIngestor(), JsonIngestor(), ParquetIngestor())


def ingestor_for(path) -> BaseIngestor | None:
    return next((i for i in INGESTORS if i.accepts(path)), None)


__all__ = [
    "INGESTORS", "BaseIngestor", "CsvIngestor", "FileProfile", "IngestionError",
    "JsonIngestor", "ParquetIngestor", "ingestor_for",
]
