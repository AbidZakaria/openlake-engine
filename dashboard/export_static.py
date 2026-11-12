"""Snapshot every API response into ``dashboard/static/data`` for static hosting.

The front end falls back to these files when ``/api`` is unreachable, so the
dashboard can be published on GitHub Pages without running the stack.

    docker compose run --rm dashboard python -m dashboard.export_static
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from dashboard.lake_data import LakeData, jsonable

OUT = Path(__file__).parent / "static" / "data"


def write(name: str, payload) -> None:
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(jsonable(payload), separators=(",", ":")), encoding="utf-8")


def main() -> None:
    if OUT.exists():
        shutil.rmtree(OUT)
    lake = LakeData(ttl=3600)
    write("overview.json", lake.overview())
    tables = lake.tables()
    write("tables.json", tables)
    write("pipeline.json", lake.pipeline())
    write("quality.json", lake.quality())
    write("analytics.json", lake.analytics())
    write("lab.json", lake.lab() or {"demos": []})
    write("benchmarks.json", lake.benchmarks() or {})
    examples = lake.examples()
    for ex in examples:
        try:
            ex["result"] = lake.run_sql(ex["sql"])
        except Exception as exc:
            ex["result"] = {"error": str(exc)}
    write("sql_examples.json", examples)
    n_snap = 0
    for t in tables:
        if t["layer"] == "lab":
            continue
        detail = lake.table_detail(t["namespace"], t["table"])
        write(f"tables/{t['namespace']}.{t['table']}.json", detail)
        if t["layer"] in ("silver", "gold"):
            for s in detail["snapshots"]:
                try:
                    write(f"snapshots/{t['namespace']}.{t['table']}.{s['snapshot_id']}.json",
                          lake.snapshot_preview(t["namespace"], t["table"], int(s["snapshot_id"])))
                    n_snap += 1
                except Exception:
                    continue
    (OUT / "STATIC_SNAPSHOT").write_text("Exported by dashboard/export_static.py\n")
    print(f"exported {len(tables)} tables, {n_snap} snapshot previews to {OUT}")


if __name__ == "__main__":
    main()
