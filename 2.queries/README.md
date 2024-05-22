# Answers

## Q1. What are the top 5 brands by receipts scanned for most recent month?

```sql
SELECT b.name, COUNT(ri.receipt_id) AS receipt_count
FROM receipts r
JOIN receipt_items ri ON r.receipt_id = ri.receipt_id
JOIN brands b ON ri.brand_code= b.brand_code
WHERE r.purchase_date >= date_trunc('month', current_date) - interval '1 month'
  AND r.purchase_date < date_trunc('month', current_date)
GROUP BY b.name
ORDER BY receipt_count DESC
LIMIT 5;

```
