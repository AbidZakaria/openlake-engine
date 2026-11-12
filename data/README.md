# data/

* `generated/` — created by `openlake generate` / `openlake simulate` (git-ignored).
  Each `batch_*` directory is one ingestion batch; `_generation_summary.json`
  lists exactly which problems were injected. `_state/` is the simulator's memory
  of in-flight orders so later batches can update them.
* `olist/` — optional: put the real [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
  CSVs here and ingest them with
  `docker compose run --rm openlake python3 -m openlake ingest --path data/olist --label olist_real`.

Nothing in this folder is read by the pipeline directly after ingestion: the
landing zone in MinIO (`s3://openlake/raw/`) is the system of record.
