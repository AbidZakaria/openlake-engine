import json

import pandas as pd
import pytest

from openlake.ingestion import CsvIngestor, IngestionError, JsonIngestor, ParquetIngestor, ingestor_for
from openlake.ingestion.batch_manager import DUPLICATE, REGISTERED, REJECTED, BatchManager, BatchManifest


def test_csv_profile(tmp_path):
    p = tmp_path / "olist_sellers_dataset.csv"
    p.write_text("seller_id,seller_state\na,SP\nb,RJ\n")
    prof = CsvIngestor().profile(p)
    assert prof.record_count == 2
    assert prof.columns == ["seller_id", "seller_state"]
    assert len(prof.sha256) == 64


def test_csv_without_rows_after_header_is_allowed_but_empty_file_is_not(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("")
    with pytest.raises(IngestionError, match="empty"):
        CsvIngestor().profile(p)


def test_mostly_ragged_csv_is_rejected(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("a,b,c\n1\n2\n3,4,5\n6\n")
    with pytest.raises(IngestionError, match="header width"):
        CsvIngestor().profile(p)


def test_json_profile_collects_union_of_keys(tmp_path):
    p = tmp_path / "r.json"
    p.write_text(json.dumps({"a": 1}) + "\n" + json.dumps({"a": 2, "b": 3}) + "\n")
    prof = JsonIngestor().profile(p)
    assert prof.record_count == 2 and prof.columns == ["a", "b"]


def test_parquet_profile(tmp_path):
    p = tmp_path / "x.parquet"
    pd.DataFrame({"order_id": ["a", "b", "c"], "v": [1.0, 2.0, 3.0]}).to_parquet(p)
    prof = ParquetIngestor().profile(p)
    assert prof.record_count == 3 and prof.columns == ["order_id", "v"]


def test_ingestor_dispatch_by_extension(tmp_path):
    assert isinstance(ingestor_for(tmp_path / "a.csv"), CsvIngestor)
    assert isinstance(ingestor_for(tmp_path / "a.ndjson"), JsonIngestor)
    assert isinstance(ingestor_for(tmp_path / "a.parquet"), ParquetIngestor)
    assert ingestor_for(tmp_path / "a.xlsx") is None


def _batch_dir(tmp_path):
    d = tmp_path / "batch"
    d.mkdir()
    (d / "olist_sellers_dataset.csv").write_text("seller_id,seller_zip_code_prefix,seller_city,seller_state\ns1,01000,sao paulo,SP\n")
    (d / "olist_customers_dataset.csv").write_text("customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\nc1,u1,01000,sao paulo,SP\n")
    (d / "notes.txt").write_text("hello")
    (d / "mystery_feed.csv").write_text("a,b\n1,2\n")
    (d / "_generation_summary.json").write_text("{}")
    return d


def test_batch_registration_and_landing_layout(tmp_path, local_store, config):
    mgr = BatchManager(local_store, config)
    m = mgr.ingest(_batch_dir(tmp_path), label="first")
    by_name = {f.filename: f for f in m.files}
    assert "_generation_summary.json" not in by_name, "underscore files are metadata, never ingested"
    assert by_name["olist_sellers_dataset.csv"].status == REGISTERED
    assert by_name["notes.txt"].status == REJECTED
    assert by_name["mystery_feed.csv"].status == REJECTED
    key = by_name["olist_sellers_dataset.csv"].key
    assert key.startswith("raw/ecommerce/sellers/ingest_date=") and f"batch_id={m.batch_id}/" in key
    assert local_store.exists(key)
    assert [p.batch_id for p in mgr.pending()] == [m.batch_id]


def test_reingesting_identical_files_is_detected(tmp_path, local_store, config):
    mgr = BatchManager(local_store, config)
    d = _batch_dir(tmp_path)
    first = mgr.ingest(d, label="first")
    second = mgr.ingest(d, label="again")
    dupes = [f for f in second.files if f.status == DUPLICATE]
    assert {f.filename for f in dupes} == {"olist_sellers_dataset.csv", "olist_customers_dataset.csv"}
    assert all(f.duplicate_of == first.batch_id for f in dupes)
    assert second.status == REJECTED  # nothing new to load (duplicates + rejected files only)


def test_manifest_roundtrip_and_mark_loaded(tmp_path, local_store, config):
    mgr = BatchManager(local_store, config)
    m = mgr.ingest(_batch_dir(tmp_path))
    f = next(f for f in m.files if f.status == REGISTERED)
    mgr.mark_loaded(m, f.key, 123, 1)
    again = BatchManifest.from_dict(json.loads(local_store.get_bytes(f"control/batches/{m.batch_id}.json")))
    loaded = next(x for x in again.files if x.key == f.key)
    assert loaded.status == "BRONZE_LOADED" and loaded.bronze_snapshot_id == 123
