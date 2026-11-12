"""Business marts derived from the star schema."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

REVENUE_STATUSES = ("delivered", "shipped", "invoiced", "processing", "approved", "created")


def _revenue(col: str) -> F.Column:
    return F.when(F.col("order_status").isin(*REVENUE_STATUSES), F.col(col)).otherwise(F.lit(0.0))


def daily_revenue(fact_orders: DataFrame) -> DataFrame:
    return fact_orders.groupBy("order_date").agg(
        F.count(F.lit(1)).alias("orders"),
        F.sum(F.col("is_delivered").cast("int")).alias("delivered_orders"),
        F.sum(F.col("is_canceled").cast("int")).alias("canceled_orders"),
        F.sum("item_count").cast("bigint").alias("items"),
        F.round(F.sum(_revenue("order_value")), 2).alias("revenue"),
        F.round(F.sum(_revenue("freight_value")), 2).alias("freight"),
        F.round(F.sum(_revenue("order_value")) / F.greatest(F.sum(F.when(~F.col("is_canceled"), 1).otherwise(0)), F.lit(1)), 2)
        .alias("avg_order_value"),
        F.round(F.avg("review_score"), 3).alias("avg_review_score"),
        F.round(F.avg(F.col("is_late").cast("double")), 4).alias("late_delivery_rate"),
    )


def category_performance(fact_items: DataFrame, dim_products: DataFrame, fact_orders: DataFrame) -> DataFrame:
    reviews = fact_orders.select("order_id", "review_score")
    return (fact_items.join(dim_products.select("product_key", "category"), "product_key", "left")
            .join(reviews, "order_id", "left")
            .withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
            .groupBy("year_month", "category").agg(
                F.countDistinct("order_id").alias("orders"),
                F.count(F.lit(1)).alias("items"),
                F.round(F.sum(_revenue("price")), 2).alias("revenue"),
                F.round(F.avg("price"), 2).alias("avg_item_price"),
                F.round(F.avg("review_score"), 3).alias("avg_review_score"),
            ))


def seller_performance(fact_items: DataFrame, fact_orders: DataFrame, dim_sellers: DataFrame) -> DataFrame:
    o = fact_orders.select("order_id", "review_score", "is_late", "delivery_days")
    agg = (fact_items.join(o, "order_id", "left").groupBy("seller_key").agg(
        F.countDistinct("order_id").alias("orders"),
        F.count(F.lit(1)).alias("items"),
        F.round(F.sum(_revenue("price")), 2).alias("revenue"),
        F.round(F.avg("review_score"), 3).alias("avg_review_score"),
        F.round(F.avg(F.col("is_late").cast("double")), 4).alias("late_delivery_rate"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.min("order_date").alias("first_sale_date"),
        F.max("order_date").alias("last_sale_date"),
    ))
    out = agg.join(dim_sellers.select("seller_key", "seller_id", "state", "region", "city"), "seller_key", "left")
    return out.withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.col("revenue").desc())))


def state_performance(fact_orders: DataFrame, dim_customers: DataFrame) -> DataFrame:
    return (fact_orders.join(dim_customers.select("customer_key", "state", "region"), "customer_key", "left")
            .groupBy("state", "region").agg(
                F.count(F.lit(1)).alias("orders"),
                F.round(F.sum(_revenue("order_value")), 2).alias("revenue"),
                F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
                F.round(F.avg(F.col("is_late").cast("double")), 4).alias("late_delivery_rate"),
                F.round(F.avg("review_score"), 3).alias("avg_review_score"),
                F.round(F.avg("freight_value"), 2).alias("avg_freight"),
            ))


def payment_mix(fact_payments: DataFrame) -> DataFrame:
    return (fact_payments.withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
            .groupBy("year_month", "payment_type").agg(
                F.count(F.lit(1)).alias("transactions"),
                F.round(F.sum("payment_value"), 2).alias("total_value"),
                F.round(F.avg("payment_installments"), 2).alias("avg_installments"),
            ))
