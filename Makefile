# OpenLake Engine — developer commands. Everything runs inside containers.
COMPOSE ?= docker compose
RUN     := $(COMPOSE) run --rm openlake
CLI     := $(RUN) python3 -m openlake
BATCH   ?= 4

.PHONY: help up down clean logs ps bootstrap generate simulate ingest bronze silver gold run quality \
        optimize showcase benchmark status query test test-unit test-spark test-integration lint \
        dashboard export-static shell

help:            ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[33m%-17s\033[0m %s\n", $$1, $$2}'

up:              ## build and start the platform; first start runs the full demo (~10 min)
	$(COMPOSE) up --build -d
	@echo "dashboard: http://localhost:8501   minio: http://localhost:9001   spark: http://localhost:8080"
	@echo "follow the bootstrap with: make logs"

down:            ## stop containers (data volumes are kept)
	$(COMPOSE) down

clean:           ## stop and delete all data (volumes, generated files, reports)
	$(COMPOSE) down -v
	rm -rf data/generated reports/*.json

logs:            ## follow the pipeline container
	$(COMPOSE) logs -f openlake

ps:
	$(COMPOSE) ps -a

bootstrap:       ## rerun the end-to-end demo even if it already ran
	$(CLI) bootstrap --force

generate:        ## generate the historical dataset
	$(CLI) generate

simulate:        ## generate CDC batch BATCH (default 4)
	$(CLI) simulate --batch $(BATCH)

ingest:          ## land every new data/generated/batch_* in MinIO
	$(CLI) ingest

bronze:          ## raw files -> Bronze Iceberg tables
	$(CLI) bronze

silver:          ## Bronze -> Silver (incremental)
	$(CLI) silver

gold:            ## Silver -> Gold (incremental)
	$(CLI) gold

run:             ## ingest + bronze + silver + gold
	$(CLI) run

quality:         ## latest quality reports
	$(CLI) quality

optimize:        ## compaction, manifest rewrite, snapshot expiry, orphan cleanup
	$(CLI) optimize

showcase:        ## Iceberg feature lab (ACID, time travel, evolution, rollback, WAP, compaction)
	$(CLI) showcase

benchmark:       ## CSV vs Parquet vs Iceberg benchmarks -> docs/benchmarks.md
	$(CLI) benchmark

status:          ## tables, snapshots, files, rows
	$(CLI) status

query:           ## DuckDB query, e.g. make query SQL="SELECT * FROM lake.gold.daily_revenue LIMIT 5"
	$(CLI) query "$(SQL)"

test:            ## every test tier (unit + Spark + integration against the stack)
	$(RUN) python3 -m pytest -q

test-unit:
	$(RUN) python3 -m pytest -q -m "not spark and not integration"

test-spark:
	$(RUN) python3 -m pytest -q -m spark

test-integration:
	$(RUN) python3 -m pytest -q -m integration

lint:
	ruff check openlake dashboard tests

export-static:   ## snapshot the dashboard API into dashboard/static/data (for GitHub Pages)
	$(COMPOSE) run --rm -v "$(CURDIR)/dashboard:/app/dashboard" dashboard python -m dashboard.export_static

shell:           ## interactive shell in the pipeline image
	$(RUN) bash
