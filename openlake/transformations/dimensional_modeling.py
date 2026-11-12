"""Star schema built from Silver.

Surrogate keys are deterministic hashes of the business key (``xxhash64``).
That keeps incremental loads simple — a fact row can be (re)computed without a
lookup against the dimension — and makes keys stable across rebuilds.
Dimensions are SCD type 1 (latest attributes win).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

REGION_SQL = """CASE
    WHEN {c} IN ('AC','AM','AP','PA','RO','RR','TO') THEN 'North'
    WHEN {c} IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE') THEN 'Northeast'
    WHEN {c} IN ('DF','GO','MS','MT') THEN 'Center-West'
    WHEN {c} IN ('ES','MG','RJ','SP') THEN 'Southeast'
    WHEN {c} IN ('PR','RS','SC') THEN 'South'
    ELSE 'Unknown' END"""


def surrogate_key(col: str) -> F.Column:
    return F.xxhash64(F.col(col))


def dim_customers(customers: DataFrame) -> DataFrame:
    return customers.select(
        surrogate_key("customer_id").alias("customer_key"),
        "customer_id", "customer_unique_id",
        F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"),
        F.expr(REGION_SQL.format(c="customer_state")).alias("region"),
        F.col("_silver_updated_at").alias("_updated_at"),
    )


def dim_sellers(sellers: DataFrame) -> DataFrame:
    return sellers.select(
        surrogate_key("seller_id").alias("seller_key"),
        "seller_id",
        F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
        F.col("seller_city").alias("city"),
        F.col("seller_state").alias("state"),
        F.expr(REGION_SQL.format(c="seller_state")).alias("region"),
        F.col("_silver_updated_at").alias("_updated_at"),
    )


def dim_products(products: DataFrame, categories: DataFrame) -> DataFrame:
    cat = categories.select("product_category_name", "product_category_name_english")
    p = products.join(cat, "product_category_name", "left")
    volume = F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")
    return p.select(
        surrogate_key("product_id").alias("product_key"),
        "product_id",
        F.col("product_category_name").alias("category_pt"),
        F.coalesce("product_category_name_english", "product_category_name", F.lit("uncategorized")).alias("category"),
        F.col("product_weight_g").alias("weight_g"),
        F.col("product_length_cm").alias("length_cm"),
        F.col("product_height_cm").alias("height_cm"),
        F.col("product_width_cm").alias("width_cm"),
        volume.cast("bigint").alias("volume_cm3"),
        F.col("product_photos_qty").alias("photos_qty"),
        F.when(F.col("product_weight_g") < 500, "light").when(F.col("product_weight_g") < 5000, "medium")
        .when(F.col("product_weight_g").isNotNull(), "heavy").otherwise("unknown").alias("weight_class"),
        F.col("_silver_updated_at").alias("_updated_at"),
    )


def dim_date(spark: SparkSession, start: str, end: str) -> DataFrame:
    days = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        F.weekofyear("date").alias("week_of_year"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.quarter("date").alias("quarter"),
        F.year("date").alias("year"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
    )


def fact_order_items(items: DataFrame, orders: DataFrame) -> DataFrame:
    o = orders.select("order_id", "customer_id", "order_status", "order_purchase_timestamp")
    return items.join(o, "order_id", "inner").select(
        "order_id", "order_item_id",
        F.to_date("order_purchase_timestamp").alias("order_date"),
        F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int").alias("order_date_key"),
        surrogate_key("product_id").alias("product_key"),
        surrogate_key("seller_id").alias("seller_key"),
        surrogate_key("customer_id").alias("customer_key"),
        "order_status", "price", "freight_value",
        F.round(F.col("price") + F.coalesce("freight_value", F.lit(0.0)), 2).alias("item_total"),
        "shipping_limit_date",
    )


def fact_orders(orders: DataFrame, items: DataFrame, payments: DataFrame, reviews: DataFrame,
                products: DataFrame, categories: DataFrame) -> DataFrame:
    cat = (products.join(categories, "product_category_name", "left")
           .select("product_id", F.coalesce("product_category_name_english", "product_category_name",
                                            F.lit("uncategorized")).alias("category")))
    item_agg = (items.join(cat, "product_id", "left").groupBy("order_id").agg(
        F.count(F.lit(1)).alias("item_count"),
        F.countDistinct("seller_id").alias("seller_count"),
        F.round(F.sum("price"), 2).alias("order_value"),
        F.round(F.sum("freight_value"), 2).alias("freight_value"),
        F.max_by("category", "price").alias("product_category"),
    ))
    pay_agg = payments.groupBy("order_id").agg(
        F.round(F.sum("payment_value"), 2).alias("payment_value"),
        F.max("payment_installments").alias("payment_installments"),
        F.max_by("payment_type", "payment_value").alias("payment_type"),
        F.count(F.lit(1)).alias("payment_count"),
    )
    review_agg = reviews.groupBy("order_id").agg(
        F.round(F.avg("review_score"), 2).alias("review_score"),
    )
    has_priority = "delivery_priority" in orders.columns
    o = orders.join(item_agg, "order_id", "left").join(pay_agg, "order_id", "left").join(review_agg, "order_id", "left")
    purchase, delivered, estimated = (F.col("order_purchase_timestamp"), F.col("order_delivered_customer_date"),
                                      F.col("order_estimated_delivery_date"))
    seconds_per_day = 86400.0
    return o.select(
        "order_id",
        surrogate_key("customer_id").alias("customer_key"),
        F.to_date(purchase).alias("order_date"),
        F.date_format(purchase, "yyyyMMdd").cast("int").alias("order_date_key"),
        purchase.alias("order_purchase_ts"),
        "order_status",
        F.coalesce("item_count", F.lit(0)).alias("item_count"),
        F.coalesce("seller_count", F.lit(0)).alias("seller_count"),
        F.coalesce("order_value", F.lit(0.0)).alias("order_value"),
        F.coalesce("freight_value", F.lit(0.0)).alias("freight_value"),
        F.round(F.coalesce("order_value", F.lit(0.0)) + F.coalesce("freight_value", F.lit(0.0)), 2).alias("total_value"),
        "payment_value", "payment_installments", "payment_type", "payment_count",
        "product_category",
        delivered.alias("delivered_at"),
        estimated.alias("estimated_delivery_at"),
        F.round((F.unix_timestamp(delivered) - F.unix_timestamp(purchase)) / seconds_per_day, 2).alias("delivery_days"),
        F.round((F.unix_timestamp(delivered) - F.unix_timestamp(estimated)) / seconds_per_day, 2).alias("delay_days"),
        F.when(delivered.isNull(), F.lit(None).cast("boolean")).otherwise(delivered > estimated).alias("is_late"),
        (F.col("order_status") == "delivered").alias("is_delivered"),
        F.col("order_status").isin("canceled", "unavailable").alias("is_canceled"),
        "review_score",
        (F.col("delivery_priority") if has_priority else F.lit(None).cast("string")).alias("delivery_priority"),
    )


def fact_payments(payments: DataFrame, orders: DataFrame) -> DataFrame:
    o = orders.select("order_id", "customer_id", "order_purchase_timestamp")
    return payments.join(o, "order_id", "inner").select(
        F.concat_ws("-", "order_id", F.col("payment_sequential").cast("string")).alias("payment_id"),
        "order_id", "payment_sequential",
        surrogate_key("customer_id").alias("customer_key"),
        F.to_date("order_purchase_timestamp").alias("order_date"),
        F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int").alias("order_date_key"),
        "payment_type", "payment_installments", "payment_value",
    )
