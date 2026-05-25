SELECT c.customer_name,
       c.email,
       m.merchant_name,
       SUM(t.amount) AS merchant_spend,
       COUNT(*) AS txn_count
FROM dim_customer c
JOIN fact_transactions t
  ON c.customer_id = t.customer_id
JOIN dim_merchant m
  ON t.merchant_id = m.merchant_id
WHERE c.tier = 'GOLD'
GROUP BY c.customer_name, c.email, m.merchant_name
ORDER BY merchant_spend DESC;
