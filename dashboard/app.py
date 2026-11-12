"""OpenLake dashboard API (FastAPI) + static single-page front end."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from dashboard.lake_data import LakeData, jsonable
from openlake.analytics.duckdb_client import QueryRejected

STATIC = Path(__file__).parent / "static"
app = FastAPI(title="OpenLake Engine", docs_url="/api/docs", openapi_url="/api/openapi.json")
lake = LakeData()


def ok(payload) -> JSONResponse:
    return JSONResponse(jsonable(payload))


def guard(fn, *args):
    try:
        return ok(fn(*args))
    except HTTPException:
        raise
    except Exception as exc:  # surface the reason instead of a bare 500
        raise HTTPException(status_code=503, detail=f"{type(exc).__name__}: {str(exc)[:400]}") from exc


@app.get("/api/health")
def health():
    return {"status": "ok", "mode": "live"}


@app.get("/api/overview")
def overview():
    return guard(lake.overview)


@app.get("/api/tables")
def tables():
    return guard(lake.tables)


@app.get("/api/tables/{ns}/{name}")
def table_detail(ns: str, name: str):
    if not lake.table_exists(ns, name):
        raise HTTPException(404, f"table {ns}.{name} not found in the catalog")
    return guard(lake.table_detail, ns, name)


@app.get("/api/tables/{ns}/{name}/snapshots/{snapshot_id}")
def snapshot(ns: str, name: str, snapshot_id: int):
    if not lake.table_exists(ns, name):
        raise HTTPException(404, f"table {ns}.{name} not found in the catalog")
    return guard(lake.snapshot_preview, ns, name, snapshot_id)


@app.get("/api/pipeline")
def pipeline():
    return guard(lake.pipeline)


@app.get("/api/quality")
def quality():
    return guard(lake.quality)


@app.get("/api/analytics")
def analytics():
    return guard(lake.analytics)


@app.get("/api/lab")
def lab():
    return ok(lake.lab() or {"demos": []})


@app.get("/api/benchmarks")
def benchmarks():
    return ok(lake.benchmarks() or {})


@app.get("/api/sql/examples")
def sql_examples():
    return ok(lake.examples())


class SqlRequest(BaseModel):
    sql: str


@app.post("/api/sql")
def run_sql(req: SqlRequest):
    try:
        return ok(lake.run_sql(req.sql))
    except QueryRejected as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(400, f"{type(exc).__name__}: {str(exc)[:600]}") from exc


@app.post("/api/refresh")
def refresh():
    lake.reset()
    return {"status": "cache cleared"}


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


app.mount("/", StaticFiles(directory=STATIC), name="static")
