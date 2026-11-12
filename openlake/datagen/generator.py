"""Deterministic generator for an Olist-shaped e-commerce dataset.

The public Olist dataset (Kaggle) cannot be downloaded without an account, so
the platform ships a generator that reproduces its *shape*: the same eight
files, the same column names (typos included), realistic distributions
(state population, category mix, payment types, delivery delays, review scores
correlated with late deliveries) and a controlled amount of dirty data so the
quality layer has something real to catch.

Drop the real Olist CSVs into ``data/olist/`` and ingest that directory
instead: the pipeline contract is identical.

Batches written by :func:`generate_initial` and :mod:`cdc_simulator`::

    batch_0001_initial/   historical load (CSV + JSON + Parquet)
    batch_0002_cdc/       new orders + status updates + new column delivery_priority
    batch_0003_cdc/       more updates, a duplicate file, and a breaking schema change
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from openlake.datagen.reference import (
    CATEGORIES,
    ORDER_STATUS_WEIGHTS,
    ORDER_STATUSES,
    REVIEW_MESSAGES,
    REVIEW_TITLES,
    STATES,
)

TS_FMT = "%Y-%m-%d %H:%M:%S"
HISTORY_START = datetime(2017, 1, 1)
HISTORY_END = datetime(2018, 8, 31, 23, 59, 59)

FILES = {
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.parquet",
    "order_reviews": "olist_order_reviews_dataset.json",
    "product_categories": "product_category_name_translation.csv",
}

ORDER_COLUMNS = [
    "order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date",
]


def _hex_ids(rng: np.random.Generator, n: int) -> list[str]:
    raw = rng.integers(0, 2**63 - 1, size=(n, 2), dtype=np.int64)
    return [f"{a:016x}{b:016x}" for a, b in raw]


def _fmt(series: pd.Series) -> pd.Series:
    return series.dt.strftime(TS_FMT).where(series.notna(), None)


def _weights(values) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    return arr / arr.sum()


@dataclass
class Universe:
    """Master data that incremental batches reuse (products, sellers)."""

    products: pd.DataFrame
    sellers: pd.DataFrame


# --------------------------------------------------------------------------- master data
def make_customers(rng: np.random.Generator, n: int, repeat_rate: float = 0.03) -> pd.DataFrame:
    states = list(STATES)
    state_idx = rng.choice(len(states), size=n, p=_weights([STATES[s][0] for s in states]))
    cust_state = [states[i] for i in state_idx]
    cities, zips = [], []
    for st in cust_state:
        _, city_list, (lo, hi), _ = STATES[st]
        cities.append(city_list[min(int(rng.exponential(1.2)), len(city_list) - 1)])
        zips.append(f"{int(rng.integers(lo, hi)):05d}")
    unique_ids = _hex_ids(rng, n)
    # Returning customers: a few customer_unique_ids appear on several customer_ids (as in Olist).
    repeaters = rng.random(n) < repeat_rate
    pool = np.flatnonzero(~repeaters)
    for i in np.flatnonzero(repeaters):
        unique_ids[i] = unique_ids[int(rng.choice(pool))]
    return pd.DataFrame({
        "customer_id": _hex_ids(rng, n),
        "customer_unique_id": unique_ids,
        "customer_zip_code_prefix": zips,
        "customer_city": cities,
        "customer_state": cust_state,
    })


def make_sellers(rng: np.random.Generator, n: int) -> pd.DataFrame:
    states = list(STATES)
    weights = np.array([STATES[s][0] for s in states])
    weights[states.index("SP")] *= 2.2  # sellers concentrate in São Paulo
    idx = rng.choice(len(states), size=n, p=_weights(weights))
    rows = []
    for sid, i in zip(_hex_ids(rng, n), idx, strict=True):
        st = states[i]
        _, cities, (lo, hi), _ = STATES[st]
        rows.append((sid, f"{int(rng.integers(lo, hi)):05d}", cities[int(rng.integers(0, len(cities)))], st))
    return pd.DataFrame(rows, columns=["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"])


def make_products(rng: np.random.Generator, n: int, sellers: pd.DataFrame) -> pd.DataFrame:
    cats = list(CATEGORIES)
    cat_idx = rng.choice(len(cats), size=n, p=_weights([CATEGORIES[c][1] for c in cats]))
    category = np.array([cats[i] for i in cat_idx], dtype=object)
    median_price = np.array([CATEGORIES[c][2] for c in category], dtype=float)
    median_weight = np.array([CATEGORIES[c][3] for c in category], dtype=float)
    price = np.round(median_price * rng.lognormal(0, 0.55, n), 2).clip(4.9, 6500)
    weight = (median_weight * rng.lognormal(0, 0.6, n)).astype(int).clip(50, 40000)
    side = np.cbrt(weight / 0.25).clip(11, 105)
    category[rng.random(n) < 0.018] = None  # ~2% uncategorised, like the source
    # Products are listed by one seller; popular sellers list more products.
    seller_pop = _weights(rng.pareto(1.3, len(sellers)) + 0.05)
    seller_ids = sellers["seller_id"].to_numpy()[rng.choice(len(sellers), size=n, p=seller_pop)]
    return pd.DataFrame({
        "product_id": _hex_ids(rng, n),
        "product_category_name": category,
        "product_name_lenght": rng.integers(10, 70, n),
        "product_description_lenght": rng.integers(40, 3000, n),
        "product_photos_qty": rng.integers(1, 8, n),
        "product_weight_g": weight,
        "product_length_cm": (side * rng.uniform(0.9, 1.6, n)).astype(int),
        "product_height_cm": (side * rng.uniform(0.4, 1.0, n)).astype(int),
        "product_width_cm": (side * rng.uniform(0.7, 1.2, n)).astype(int),
        # private columns (not written to the product file)
        "_price": price,
        "_seller_id": seller_ids,
        "_popularity": rng.pareto(1.8, n) + 0.05,
    })


def make_categories() -> pd.DataFrame:
    return pd.DataFrame(
        [(pt, en) for pt, (en, *_rest) in CATEGORIES.items()],
        columns=["product_category_name", "product_category_name_english"],
    )


# --------------------------------------------------------------------------- transactions
def purchase_timestamps(rng: np.random.Generator, n: int, start: datetime, end: datetime,
                        growth: float = 3.0) -> pd.Series:
    days = (end.date() - start.date()).days + 1
    day_index = np.arange(days)
    dates = pd.date_range(start.date(), periods=days, freq="D")
    weight = 1 + growth * day_index / max(days - 1, 1)                   # business growth
    weight *= np.where(dates.dayofweek >= 5, 0.78, 1.0)                  # quieter weekends
    weight *= np.where(dates.month == 12, 1.15, 1.0)                     # holiday season
    for spike, factor in ((datetime(2017, 11, 24), 5.0), (datetime(2017, 11, 25), 2.2),
                          (datetime(2018, 5, 13), 1.6)):                 # Black Friday, Mother's Day
        weight = np.where(dates == pd.Timestamp(spike), weight * factor, weight)
    chosen = rng.choice(days, size=n, p=_weights(weight))
    hour_weights = _weights([1, .5, .3, .2, .2, .3, .8, 1.6, 3, 4, 4.6, 4.8, 4.6, 4.8, 4.9, 4.7, 4.6,
                             4.5, 4.3, 4.4, 4.7, 4.6, 4.0, 2.6])
    hours = rng.choice(24, size=n, p=hour_weights)
    seconds = rng.integers(0, 3600, n)
    ts = (pd.to_datetime(dates[chosen]) + pd.to_timedelta(hours, unit="h") + pd.to_timedelta(seconds, unit="s"))
    return pd.Series(ts).clip(upper=pd.Timestamp(end))


def build_orders(rng: np.random.Generator, customers: pd.DataFrame, purchase: pd.Series,
                 horizon: datetime, in_flight_days: int = 21) -> pd.DataFrame:
    n = len(customers)
    tier = customers["customer_state"].map(lambda s: STATES[s][3]).to_numpy()
    status = rng.choice(ORDER_STATUSES, size=n, p=_weights(ORDER_STATUS_WEIGHTS)).astype(object)
    # Orders placed in the last weeks before the horizon are still in flight.
    recent = (pd.Timestamp(horizon) - purchase).dt.days.to_numpy() < in_flight_days
    in_flight = rng.choice(["shipped", "processing", "invoiced", "delivered"], size=n, p=[0.55, 0.12, 0.08, 0.25])
    status = np.where(recent & (status == "delivered"), in_flight, status)

    approved = purchase + pd.to_timedelta(rng.exponential(10, n).clip(0.05, 96), unit="h")
    carrier = approved + pd.to_timedelta(rng.gamma(2.0, 1.4, n).clip(0.2, 20), unit="D")
    transit = rng.gamma(2.2 + 1.6 * tier, 2.4, n)
    # Carrier disruptions (strikes, lost parcels, holiday backlog) cause most late deliveries.
    disrupted = rng.random(n) < 0.055
    transit = (transit + np.where(disrupted, rng.uniform(6, 28, n), 0)).clip(0.5, 90)
    delivered = carrier + pd.to_timedelta(transit, unit="D")
    est_days = (16 + 5 * tier + rng.integers(0, 9, n)).astype(float)
    estimated = (purchase + pd.to_timedelta(est_days, unit="D")).dt.normalize()

    not_approved = np.isin(status, ["created"]) | ((status == "canceled") & (rng.random(n) < 0.4))
    not_shipped = np.isin(status, ["created", "approved", "invoiced", "processing", "canceled", "unavailable"])
    not_delivered = status != "delivered"
    approved = approved.where(~not_approved, pd.NaT)
    carrier = carrier.where(~not_shipped, pd.NaT)
    delivered = delivered.where(~not_delivered, pd.NaT)
    # Anything "delivered" in the future relative to the horizon is still shipped.
    future = delivered > pd.Timestamp(horizon)
    status = np.where(future, "shipped", status)
    delivered = delivered.where(~future, pd.NaT)

    return pd.DataFrame({
        "order_id": _hex_ids(rng, n),
        "customer_id": customers["customer_id"].to_numpy(),
        "order_status": status,
        "order_purchase_timestamp": purchase.to_numpy(),
        "order_approved_at": approved.to_numpy(),
        "order_delivered_carrier_date": carrier.to_numpy(),
        "order_delivered_customer_date": delivered.to_numpy(),
        "order_estimated_delivery_date": estimated.to_numpy(),
        "_tier": tier,
    })


def build_items(rng: np.random.Generator, orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    n = len(orders)
    counts = rng.choice([1, 2, 3, 4, 5], size=n, p=[0.885, 0.075, 0.025, 0.01, 0.005])
    order_idx = np.repeat(np.arange(n), counts)
    item_seq = np.concatenate([np.arange(1, c + 1) for c in counts])
    prod_idx = rng.choice(len(products), size=len(order_idx), p=_weights(products["_popularity"]))
    # Multi-item orders frequently repeat the same product (as Olist does).
    repeat = (item_seq > 1) & (rng.random(len(order_idx)) < 0.6)
    prod_idx[repeat] = prod_idx[np.flatnonzero(repeat) - 1]
    prod = products.iloc[prod_idx]
    o = orders.iloc[order_idx]
    price = np.round(prod["_price"].to_numpy() * rng.uniform(0.92, 1.08, len(order_idx)), 2)
    weight = prod["product_weight_g"].to_numpy()
    freight = np.round(7.5 + weight / 1000 * (2.2 + 1.3 * o["_tier"].to_numpy()) + rng.gamma(2, 2.2, len(order_idx)), 2)
    shipping_limit = pd.to_datetime(o["order_purchase_timestamp"].to_numpy()) + pd.to_timedelta(
        rng.integers(3, 8, len(order_idx)), unit="D")
    return pd.DataFrame({
        "order_id": o["order_id"].to_numpy(),
        "order_item_id": item_seq,
        "product_id": prod["product_id"].to_numpy(),
        "seller_id": prod["_seller_id"].to_numpy(),
        "shipping_limit_date": shipping_limit,
        "price": price,
        "freight_value": freight,
    })


def build_payments(rng: np.random.Generator, orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    totals = (items.assign(total=items["price"] + items["freight_value"])
              .groupby("order_id", sort=False)["total"].sum())
    totals = totals.reindex(orders["order_id"]).fillna(0).round(2)
    rows = []
    kinds = rng.choice(["credit_card", "boleto", "voucher", "debit_card"], size=len(totals), p=[0.74, 0.195, 0.05, 0.015])
    for (order_id, total), kind in zip(totals.items(), kinds, strict=True):
        if kind == "credit_card":
            inst = int(rng.choice([1, 2, 3, 4, 5, 6, 8, 10], p=[0.5, 0.12, 0.1, 0.07, 0.06, 0.05, 0.05, 0.05]))
            if total > 0 and rng.random() < 0.03:  # card + voucher split
                v = round(min(total * rng.uniform(0.1, 0.5), total), 2)
                rows.append((order_id, 1, "credit_card", inst, round(total - v, 2)))
                rows.append((order_id, 2, "voucher", 1, v))
            else:
                rows.append((order_id, 1, "credit_card", inst, total))
        elif kind == "voucher":
            parts = int(rng.choice([1, 2, 3], p=[0.7, 0.2, 0.1]))
            cuts = np.sort(rng.uniform(0, total, parts - 1)) if parts > 1 else np.array([])
            amounts = np.diff(np.concatenate([[0], cuts, [total]])).round(2)
            for seq, amt in enumerate(amounts, start=1):
                rows.append((order_id, seq, "voucher", 1, float(amt)))
        else:
            rows.append((order_id, 1, kind, 1, total))
    return pd.DataFrame(rows, columns=["order_id", "payment_sequential", "payment_type",
                                       "payment_installments", "payment_value"])


def build_reviews(rng: np.random.Generator, orders: pd.DataFrame) -> pd.DataFrame:
    eligible = orders[orders["order_status"].isin(["delivered", "canceled", "unavailable"])]
    eligible = eligible[rng.random(len(eligible)) < 0.985]
    n = len(eligible)
    delivered = pd.to_datetime(eligible["order_delivered_customer_date"])
    estimated = pd.to_datetime(eligible["order_estimated_delivery_date"])
    late = (delivered > estimated).fillna(False).to_numpy()
    failed = eligible["order_status"].isin(["canceled", "unavailable"]).to_numpy()
    happy = rng.choice([5, 4, 3, 2, 1], size=n, p=[0.6, 0.2, 0.08, 0.03, 0.09])
    unhappy = rng.choice([5, 4, 3, 2, 1], size=n, p=[0.12, 0.08, 0.14, 0.14, 0.52])
    score = np.where(late | failed, unhappy, happy)
    base = delivered.fillna(pd.to_datetime(eligible["order_purchase_timestamp"]) + pd.Timedelta(days=12))
    created = (base + pd.to_timedelta(rng.integers(0, 3, n), unit="D")).dt.normalize()
    answered = created + pd.to_timedelta(rng.exponential(30, n).clip(1, 400), unit="h")
    has_comment = rng.random(n) < np.where(score <= 2, 0.75, 0.35)
    msg_idx = np.where(score >= 4, rng.choice([0, 1, 2, 6, 7, 9], n), rng.choice([3, 4, 5, 8], n))
    has_title = rng.random(n) < 0.12
    title_idx = np.where(score >= 4, rng.choice([0, 1, 2, 3, 6, 7, 9], n), rng.choice([4, 5, 8], n))
    return pd.DataFrame({
        "review_id": _hex_ids(rng, n),
        "order_id": eligible["order_id"].to_numpy(),
        "review_score": score,
        "review_comment_title": [REVIEW_TITLES[i] if t else None for i, t in zip(title_idx, has_title, strict=True)],
        "review_comment_message": [REVIEW_MESSAGES[i] if c else None for i, c in zip(msg_idx, has_comment, strict=True)],
        "review_creation_date": created.to_numpy(),
        "review_answer_timestamp": answered.to_numpy(),
    })


# --------------------------------------------------------------------------- dirty data
def inject_dirty_data(rng: np.random.Generator, frames: dict[str, pd.DataFrame]) -> dict[str, dict[str, int]]:
    """Corrupt a small, known number of records so the quality layer has work to do."""
    log: dict[str, dict[str, int]] = {}

    def pick(df: pd.DataFrame, k: int) -> np.ndarray:
        return rng.choice(df.index.to_numpy(), size=min(k, len(df)), replace=False)

    cust = frames["customers"]
    idx = pick(cust, 60)
    cust.loc[idx[:40], "customer_state"] = cust.loc[idx[:40], "customer_state"].str.lower().map(lambda s: f" {s} ")
    cust.loc[idx[40:], "customer_state"] = "XX"
    log["customers"] = {"messy_state_case_fixed_by_standardization": 40, "invalid_state": 20}

    orders = frames["orders"]
    idx = pick(orders, 45)
    orders.loc[idx[:15], "order_status"] = "lost_in_transit"
    orders.loc[idx[15:25], "customer_id"] = None
    orders.loc[idx[25:45], "order_status"] = orders.loc[idx[25:45], "order_status"].str.upper()
    dupes = orders.loc[pick(orders, 150)]
    frames["orders"] = pd.concat([orders, dupes], ignore_index=True)
    log["orders"] = {"invalid_status": 15, "null_customer_id": 10, "uppercase_status_fixed": 20, "exact_duplicates": 150}

    items = frames["order_items"]
    idx = pick(items, 35)
    items.loc[idx[:25], "price"] = -items.loc[idx[:25], "price"]
    items.loc[idx[25:], "product_id"] = None
    log["order_items"] = {"negative_price": 25, "null_product_id": 10}

    pay = frames["order_payments"]
    idx = pick(pay, 25)
    pay.loc[idx[:20], "payment_value"] = -pay.loc[idx[:20], "payment_value"].abs() - 1
    pay.loc[idx[20:], "payment_type"] = "pix"
    log["order_payments"] = {"negative_value": 20, "unknown_payment_type": 5}

    rev = frames["order_reviews"]
    idx = pick(rev, 15)
    rev.loc[idx, "review_score"] = rng.choice([0, 6], size=len(idx))
    log["order_reviews"] = {"score_out_of_range": 15}
    return log


# --------------------------------------------------------------------------- writers
def write_batch(frames: dict[str, pd.DataFrame], out_dir: Path, malformed_ts: int = 0,
                rng: np.random.Generator | None = None) -> dict[str, int]:
    """Write frames using the Olist file names and formats. Returns row counts."""
    out_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for entity, df in frames.items():
        df = df[[c for c in df.columns if not c.startswith("_")]].copy()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = _fmt(df[col])
        if entity == "orders" and malformed_ts and rng is not None:
            idx = rng.choice(df.index.to_numpy(), size=malformed_ts, replace=False)
            df.loc[idx, "order_purchase_timestamp"] = "2018-13-45 25:61:00"
        name = FILES.get(entity, f"{entity}.csv")
        path = out_dir / name
        if name.endswith(".parquet"):
            df.to_parquet(path, index=False)
        elif name.endswith(".json"):
            with open(path, "w", encoding="utf-8") as fh:
                for rec in df.to_dict(orient="records"):
                    fh.write(json.dumps({k: (None if (isinstance(v, float) and np.isnan(v)) else v)
                                         for k, v in rec.items()}, default=str) + "\n")
        else:
            df.to_csv(path, index=False)
        counts[entity] = len(df)
    return counts


def save_state(state_dir: Path, orders: pd.DataFrame, universe: Universe) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    orders.to_parquet(state_dir / "orders.parquet", index=False)
    universe.products.to_parquet(state_dir / "products.parquet", index=False)
    universe.sellers.to_parquet(state_dir / "sellers.parquet", index=False)


def generate_initial(out_root: Path, n_orders: int = 100_000, seed: int = 42) -> dict:
    """Generate the historical batch and the simulator state. Returns a summary."""
    rng = np.random.default_rng(seed)
    sellers = make_sellers(rng, max(50, int(n_orders * 0.031)))
    products = make_products(rng, max(200, int(n_orders * 0.33)), sellers)
    customers = make_customers(rng, n_orders)
    purchase = purchase_timestamps(rng, n_orders, HISTORY_START, HISTORY_END)
    purchase = purchase.sort_values().reset_index(drop=True)
    orders = build_orders(rng, customers, purchase, HISTORY_END)
    items = build_items(rng, orders, products)
    payments = build_payments(rng, orders, items)
    reviews = build_reviews(rng, orders)

    frames = {
        "product_categories": make_categories(),
        "customers": customers,
        "sellers": sellers,
        "products": products,
        "orders": orders,
        "order_items": items,
        "order_payments": payments,
        "order_reviews": reviews,
    }
    clean_orders = orders.copy()
    dirty_log = inject_dirty_data(rng, frames)
    batch_dir = out_root / "batch_0001_initial"
    counts = write_batch(frames, batch_dir, malformed_ts=12, rng=rng)
    dirty_log["orders"]["malformed_purchase_timestamp"] = 12

    save_state(out_root / "_state", clean_orders.assign(delivery_priority=None), Universe(products, sellers))
    summary = {"batch": batch_dir.name, "seed": seed, "rows": counts, "injected_issues": dirty_log}
    (batch_dir / "_generation_summary.json").write_text(json.dumps(summary, indent=2))
    return summary
