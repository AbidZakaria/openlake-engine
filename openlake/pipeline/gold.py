"""Gold: dimensional model + business marts, refreshed incrementally.

* Dimensions are upserted from the Silver rows changed since the last Gold run.
* Facts are recomputed only for *affected orders* — any order whose header,
  items, payments or reviews changed — and MERGEd by their grain key.
* ``daily_revenue`` is partitioned by month; only the months touched by
  affected orders are recomputed and swapped in atomically with a dynamic
  partition overwrite.
* Small marts (category, seller, state, payment mix) are recomputed from Gold
  and replaced in a single atomic overwrite commit.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from openlake.config.pipeline import PipelineConfig
from openlake.iceberg.catalog_manager import CatalogManager
from openlake.pipeline import writer
from openlake.pipeline.ops import OpsLog
from openlake.transformations import aggregations as agg
from openlake.transformations import dimensional_modeling as dm

FACT_SOURCES = ("orders", "order_items", "order_payments", "order_reviews")


class GoldBuilder:
    def __init__(self, spark: SparkSession, config: PipelineConfig, ops: OpsLog):
        self.spark = spark
        self.config = config
        self.ops = ops
        self.cm = CatalogManager(spark, config)
        self.watermark = ops.get_watermark("gold", "silver")
        self.full = self.watermark is None

    # ------------------------------------------------------------------ helpers
    def silver(self, name: str) -> DataFrame:
        return self.spark.table(self.config.table("silver", name))

    def changed(self, name: str) -> DataFrame:
        df = self.silver(name)
        if self.full:
            return df
        return df.filter(F.col("_silver_updated_at") > F.lit(self.watermark).cast("timestamp"))

    def gold(self, name: str) -> str:
        return self.config.table("gold", name)

    def partitions(self, name: str) -> tuple[str, ...]:
        return self.config.gold_partitions.get(name, ())

    def high_water(self) -> str | None:
        parts = [self.silver(n).agg(F.max("_silver_updated_at").alias("m")) for n in
                 [e.name for e in self.config.ordered_entities()] if self.cm.exists(self.config.table("silver", n))]
        if not parts:
            return None
        union = parts[0]
        for p in parts[1:]:
            union = union.unionByName(p)
        value = union.agg(F.max("m").cast("string").alias("m")).collect()[0]["m"]
        return value

    def replace(self, name: str, df: DataFrame, comment: str) -> int | None:
        """Atomically replace a small mart (one overwrite snapshot)."""
        table = self.gold(name)
        if not self.cm.exists(table):
            return writer.append(self.cm, table, df, self.partitions(name), comment)
        writer.evolve_table(self.cm, table, df)
        writer.align_to_table(self.cm, table, df).writeTo(table).overwrite(F.lit(True))
        return self.cm.current_snapshot_id(table)

    # --------------------------------------------------------------- dimensions
    def build_dimensions(self) -> None:
        cats = self.silver("product_categories")
        specs = [
            ("dim_customers", ["customer_key"], lambda: dm.dim_customers(self.changed("customers")),
             "Customer dimension (SCD1)"),
            ("dim_sellers", ["seller_key"], lambda: dm.dim_sellers(self.changed("sellers")), "Seller dimension (SCD1)"),
        ]
        categories_changed = self.full or self.changed("product_categories").limit(1).count() > 0
        products_src = self.silver("products") if categories_changed else self.changed("products")
        specs.append(("dim_products", ["product_key"], lambda: dm.dim_products(products_src, cats),
                      "Product dimension with English category names (SCD1)"))
        for name, keys, build, comment in specs:
            table = self.gold(name)
            with self.ops.step(name, table) as step:
                df = build().cache()
                step.input_records = df.count()
                if step.input_records:
                    step.snapshot_id = writer.merge_upsert(self.cm, table, df, keys, comment=comment)
                else:
                    step.details = {"skipped": True, "reason": "no changed source rows"}
                step.output_records = step.input_records
                df.unpersist()

        table = self.gold("dim_date")
        with self.ops.step("dim_date", table) as step:
            bounds = self.silver("orders").agg(
                F.min(F.to_date("order_purchase_timestamp")).cast("string").alias("lo"),
                F.max(F.greatest(F.to_date("order_purchase_timestamp"),
                                 F.to_date("order_estimated_delivery_date"))).cast("string").alias("hi"),
            ).collect()[0]
            dates = dm.dim_date(self.spark, bounds["lo"], bounds["hi"])
            if self.cm.exists(table):
                dates = dates.join(self.spark.table(table).select("date_key"), "date_key", "left_anti")
            new = dates.count()
            step.input_records = step.output_records = new
            if new or not self.cm.exists(table):
                step.snapshot_id = writer.append(self.cm, table, dates, comment="Calendar dimension")
            step.details = {"range": [bounds["lo"], bounds["hi"]]}

    # -------------------------------------------------------------------- facts
    def affected_orders(self) -> DataFrame:
        ids = None
        for name in FACT_SOURCES:
            part = self.changed(name).select("order_id")
            ids = part if ids is None else ids.unionByName(part)
        return ids.distinct()

    def build_facts(self) -> list[str]:
        affected = self.affected_orders().cache()
        n_affected = affected.count()

        def scoped(name: str) -> DataFrame:
            return self.silver(name).join(affected, "order_id", "left_semi")

        orders = scoped("orders")
        items = scoped("order_items")
        payments = scoped("order_payments")
        reviews = scoped("order_reviews")
        facts = [
            ("fact_orders", ["order_id"],
             lambda: dm.fact_orders(orders, items, payments, reviews, self.silver("products"),
                                    self.silver("product_categories")),
             "Order-grain fact: value, payments, delivery performance, review"),
            ("fact_order_items", ["order_id", "order_item_id"], lambda: dm.fact_order_items(items, orders),
             "Order-item-grain fact: price and freight per product and seller"),
            ("fact_payments", ["payment_id"], lambda: dm.fact_payments(payments, orders),
             "Payment-grain fact: method, installments and value"),
        ]
        for name, keys, build, comment in facts:
            table = self.gold(name)
            with self.ops.step(name, table) as step:
                step.details = {"affected_orders": n_affected, "mode": "full" if self.full else "incremental"}
                if n_affected == 0:
                    step.details["skipped"] = True
                    continue
                df = build().cache()
                step.input_records = df.count()
                step.snapshot_id = writer.merge_upsert(self.cm, table, df, keys, self.partitions(name), comment)
                step.output_records = step.input_records
                df.unpersist()

        months = []
        if n_affected:
            months = [r["m"] for r in self.spark.table(self.gold("fact_orders"))
                      .join(affected, "order_id", "left_semi")
                      .select(F.date_format("order_date", "yyyy-MM").alias("m")).distinct().collect()]
        affected.unpersist()
        return sorted(m for m in months if m)

    # -------------------------------------------------------------------- marts
    def build_marts(self, months: list[str]) -> None:
        fo = self.spark.table(self.gold("fact_orders"))
        table = self.gold("daily_revenue")
        with self.ops.step("daily_revenue", table) as step:
            step.details = {"months_recomputed": months}
            if not months and self.cm.exists(table):
                step.details["skipped"] = True
            else:
                scope = fo if not self.cm.exists(table) else fo.filter(
                    F.date_format("order_date", "yyyy-MM").isin(months))
                df = agg.daily_revenue(scope).cache()
                step.input_records = step.output_records = df.count()
                if not self.cm.exists(table):
                    step.snapshot_id = writer.append(self.cm, table, df, self.partitions("daily_revenue"),
                                                     "Daily revenue KPIs (partitioned by month)")
                else:
                    # Dynamic partition overwrite: only the recomputed months are replaced.
                    writer.align_to_table(self.cm, table, df).writeTo(table).overwritePartitions()
                    step.snapshot_id = self.cm.current_snapshot_id(table)
                df.unpersist()

        fi = self.spark.table(self.gold("fact_order_items"))
        fp = self.spark.table(self.gold("fact_payments"))
        dp = self.spark.table(self.gold("dim_products"))
        ds = self.spark.table(self.gold("dim_sellers"))
        dc = self.spark.table(self.gold("dim_customers"))
        marts = [
            ("category_performance", lambda: agg.category_performance(fi, dp, fo), "Monthly revenue per category"),
            ("seller_performance", lambda: agg.seller_performance(fi, fo, ds), "Seller scorecard with revenue rank"),
            ("state_performance", lambda: agg.state_performance(fo, dc), "Delivery and revenue KPIs per state"),
            ("payment_mix", lambda: agg.payment_mix(fp), "Monthly payment method mix"),
        ]
        for name, build, comment in marts:
            with self.ops.step(name, self.gold(name)) as step:
                df = build().cache()
                step.input_records = step.output_records = df.count()
                step.snapshot_id = self.replace(name, df, comment)
                df.unpersist()

    def run(self) -> dict:
        high_water = self.high_water()
        if high_water is None:
            return {"skipped": True, "reason": "silver is empty"}
        if not self.full and high_water <= self.watermark:
            with self.ops.step("gold", None) as step:
                step.details = {"skipped": True, "reason": "no silver changes since last run",
                                "watermark": self.watermark}
            return {"skipped": True}
        self.build_dimensions()
        months = self.build_facts()
        self.build_marts(months)
        self.ops.set_watermark("gold", "silver", high_water)
        return {"mode": "full" if self.full else "incremental", "months_recomputed": len(months),
                "watermark": high_water}


def run_gold(spark: SparkSession, config: PipelineConfig, ops: OpsLog) -> dict:
    return GoldBuilder(spark, config, ops).run()
