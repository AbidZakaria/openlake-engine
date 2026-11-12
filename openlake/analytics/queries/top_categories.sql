-- Top product categories by revenue (joins the star schema)
SELECT
    p.category,
    count(DISTINCT i.order_id)      AS orders,
    round(sum(i.price), 2)          AS revenue,
    round(avg(o.review_score), 2)   AS avg_review
FROM lake.gold.fact_order_items i
JOIN lake.gold.dim_products p USING (product_key)
JOIN lake.gold.fact_orders  o USING (order_id)
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY p.category
ORDER BY revenue DESC
LIMIT 10;
