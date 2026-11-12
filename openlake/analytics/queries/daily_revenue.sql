-- Daily revenue, order count and average order value (DuckDB over Iceberg)
SELECT
    order_date,
    revenue                AS daily_revenue,
    orders                 AS total_orders,
    avg_order_value,
    late_delivery_rate
FROM lake.gold.daily_revenue
ORDER BY order_date;
