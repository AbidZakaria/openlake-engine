-- Delivery performance by customer region: the further from São Paulo, the later and less happy
SELECT
    c.region,
    count(*)                                   AS orders,
    round(avg(o.delivery_days), 1)             AS avg_delivery_days,
    round(100 * avg(o.is_late::INT), 1)        AS late_pct,
    round(avg(o.review_score), 2)              AS avg_review
FROM lake.gold.fact_orders o
JOIN lake.gold.dim_customers c USING (customer_key)
WHERE o.is_delivered
GROUP BY c.region
ORDER BY avg_delivery_days;
