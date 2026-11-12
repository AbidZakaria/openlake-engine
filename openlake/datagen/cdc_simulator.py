"""Change-data simulation: produce incremental batches after the historical load.

Each call to :func:`simulate_batch` moves the business clock forward two weeks
and writes a batch directory containing

* brand-new orders (with their customers, items, payments and reviews),
* full-row *updates* for orders that were still in flight
  (``shipped`` -> ``delivered``, some ``processing`` -> ``canceled``),
* reviews for the orders that just got delivered.

Batch 2 additionally introduces a new upstream column, ``delivery_priority``,
on the orders feed (an additive, compatible schema change). Batch 3 ships a
sellers file whose ``seller_state`` column was renamed to ``state`` (a breaking
change the Silver contract must reject) and re-sends a file that was already
ingested (exercising duplicate-file detection).
"""

from __future__ import annotations

import json
import shutil
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from openlake.datagen.generator import (
    FILES,
    HISTORY_END,
    ORDER_COLUMNS,
    STATES,
    Universe,
    build_items,
    build_payments,
    build_reviews,
    make_customers,
    make_sellers,
    purchase_timestamps,
    save_state,
    write_batch,
)
from openlake.datagen.generator import build_orders as _build_orders

IN_FLIGHT = ["created", "approved", "invoiced", "processing", "shipped"]
PRIORITIES = ["standard", "express", "same_day"]


def _load_state(state_dir: Path) -> tuple[pd.DataFrame, Universe]:
    if not (state_dir / "orders.parquet").exists():
        raise FileNotFoundError("simulator state missing — run `openlake generate` first")
    orders = pd.read_parquet(state_dir / "orders.parquet")
    products = pd.read_parquet(state_dir / "products.parquet")
    sellers = pd.read_parquet(state_dir / "sellers.parquet")
    return orders, Universe(products, sellers)


def _progress_orders(rng: np.random.Generator, orders: pd.DataFrame, horizon: pd.Timestamp,
                     deliver_rate: float = 0.85, cancel_rate: float = 0.05) -> pd.DataFrame:
    """Advance in-flight orders. Returns only the rows that changed."""
    open_orders = orders[orders["order_status"].isin(IN_FLIGHT)].copy()
    if open_orders.empty:
        return open_orders
    roll = rng.random(len(open_orders))
    deliver = roll < deliver_rate
    cancel = (roll >= deliver_rate) & (roll < deliver_rate + cancel_rate)
    purchase = pd.to_datetime(open_orders["order_purchase_timestamp"])
    approved = pd.to_datetime(open_orders["order_approved_at"]).fillna(purchase + pd.Timedelta(hours=8))
    carrier = pd.to_datetime(open_orders["order_delivered_carrier_date"]).fillna(approved + pd.Timedelta(days=2))
    tier = open_orders["_tier"].to_numpy() if "_tier" in open_orders else np.full(len(open_orders), 1)
    transit = rng.gamma(2.2 + 1.6 * tier, 2.4) + np.where(rng.random(len(open_orders)) < 0.055,
                                                           rng.uniform(6, 28, len(open_orders)), 0)
    delivered = carrier + pd.to_timedelta(transit.clip(0.5, 90), unit="D")
    delivered = delivered.where(delivered <= horizon, horizon - pd.to_timedelta(rng.uniform(0, 48, len(delivered)), unit="h"))
    delivered = delivered.where(delivered > carrier, carrier + pd.Timedelta(hours=6))

    open_orders.loc[deliver, "order_status"] = "delivered"
    open_orders.loc[deliver, "order_approved_at"] = approved[deliver]
    open_orders.loc[deliver, "order_delivered_carrier_date"] = carrier[deliver]
    open_orders.loc[deliver, "order_delivered_customer_date"] = delivered[deliver]
    open_orders.loc[cancel, "order_status"] = "canceled"
    if "delivery_priority" not in open_orders or open_orders["delivery_priority"].isna().all():
        open_orders["delivery_priority"] = rng.choice(PRIORITIES, size=len(open_orders), p=[0.8, 0.17, 0.03])
    return open_orders[deliver | cancel]


def simulate_batch(out_root: Path, batch_no: int, new_orders: int = 2000, seed: int = 7) -> dict:
    if batch_no < 2:
        raise ValueError("batch 1 is the historical load; simulated batches start at 2")
    rng = np.random.default_rng(seed + batch_no)
    state_dir = out_root / "_state"
    orders_state, universe = _load_state(state_dir)
    start = HISTORY_END + timedelta(seconds=1) + timedelta(days=14 * (batch_no - 2))
    horizon = start + timedelta(days=14) - timedelta(seconds=1)

    # 1. updates to orders that were in flight
    updates = _progress_orders(rng, orders_state, pd.Timestamp(horizon))

    # 2. brand new orders
    customers = make_customers(rng, new_orders)
    purchase = purchase_timestamps(rng, new_orders, start, horizon, growth=0.2)
    new = _build_orders(rng, customers, purchase.sort_values().reset_index(drop=True), horizon, in_flight_days=8)
    new["delivery_priority"] = rng.choice(PRIORITIES, size=len(new), p=[0.8, 0.17, 0.03])
    items = build_items(rng, new, universe.products)
    payments = build_payments(rng, new, items)

    # 3. reviews for everything that reached a final state in this batch
    reviews = build_reviews(rng, pd.concat([new, updates], ignore_index=True))

    orders_out = pd.concat([new, updates], ignore_index=True)[ORDER_COLUMNS + ["delivery_priority"]]
    frames = {
        "customers": customers,
        "orders": orders_out,
        "order_items": items,
        "order_payments": payments,
        "order_reviews": reviews,
    }
    batch_dir = out_root / f"batch_{batch_no:04d}_cdc"
    if batch_dir.exists():
        shutil.rmtree(batch_dir)
    counts = write_batch(frames, batch_dir)
    notes = {"new_orders": len(new), "status_updates": len(updates),
             "delivered_updates": int((updates["order_status"] == "delivered").sum()) if len(updates) else 0,
             "canceled_updates": int((updates["order_status"] == "canceled").sum()) if len(updates) else 0,
             "schema_change": "orders gains column delivery_priority (additive)" if batch_no == 2 else None}

    if batch_no == 3:
        # Breaking change: upstream renamed seller_state -> state.
        drifted = make_sellers(rng, 25).rename(columns={"seller_state": "state"})
        drifted.to_csv(batch_dir / FILES["sellers"], index=False)
        counts["sellers"] = len(drifted)
        notes["schema_change"] = "sellers renames seller_state -> state (breaking)"
        # Re-delivery of a file that was already ingested in batch 1.
        initial = out_root / "batch_0001_initial" / FILES["product_categories"]
        if initial.exists():
            shutil.copyfile(initial, batch_dir / FILES["product_categories"])
            notes["duplicate_file"] = FILES["product_categories"]

    # 4. persist simulator state
    merged = orders_state.set_index("order_id")
    upd = updates.set_index("order_id")
    merged.update(upd)
    merged = pd.concat([merged.reset_index(), new.assign(_tier=new["_tier"])], ignore_index=True)
    universe.sellers = pd.concat([universe.sellers], ignore_index=True)
    save_state(state_dir, merged, universe)

    summary = {"batch": batch_dir.name, "window": [str(start), str(horizon)], "rows": counts, "notes": notes}
    (batch_dir / "_generation_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    return summary


__all__ = ["simulate_batch", "STATES"]
