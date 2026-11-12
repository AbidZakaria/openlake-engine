-- Payment method distribution
SELECT
    payment_type,
    count(*)                      AS transactions,
    round(sum(payment_value), 2)  AS total_value,
    round(avg(payment_installments), 2) AS avg_installments
FROM lake.gold.fact_payments
GROUP BY payment_type
ORDER BY total_value DESC;
