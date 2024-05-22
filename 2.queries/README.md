# Answers ([Follow this instructions to execute the queries](#execute-the-queries-using-docker-compose))

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

## Q2. Ranking comparison of top 5 brands by receipts scanned for recent month and previous month

```sql
WITH recent_month AS (
  SELECT b.name, COUNT(ri.receipt_id) AS receipt_count
  FROM receipts r
  JOIN receipt_items ri ON r.receipt_id = ri.receipt_id
  JOIN brands b ON ri.brand_code = b.brand_code
  WHERE r.purchase_date >= date_trunc('month', current_date) - interval '1 month'
    AND r.purchase_date < date_trunc('month', current_date)
  GROUP BY b.name
  ORDER BY receipt_count DESC
  LIMIT 5
),
previous_month AS (
  SELECT b.name, COUNT(ri.receipt_id) AS receipt_count
  FROM receipts r
  JOIN receipt_items ri ON r.receipt_id = ri.receipt_id
  JOIN brands b ON ri.brand_code = b.brand_code
  WHERE r.purchase_date >= date_trunc('month', current_date) - interval '2 month'
    AND r.purchase_date < date_trunc('month', current_date) - interval '1 month'
  GROUP BY b.name
  ORDER BY receipt_count DESC
  LIMIT 5
)
SELECT r.name AS recent_month_brand, r.receipt_count AS recent_month_count,
       p.name AS previous_month_brand, p.receipt_count AS previous_month_count
FROM recent_month r
FULL OUTER JOIN previous_month p ON r.name = p.name
ORDER BY recent_month_count DESC, previous_month_count DESC;
```

## Q3. Average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’

```sql
SELECT rewards_receipt_status, AVG(total_spent) AS average_spend
FROM receipts
WHERE rewards_receipt_status IN ('Accepted', 'Rejected')
GROUP BY rewards_receipt_status;
```

## Q4. Total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’

```sql
SELECT r.rewards_receipt_status, SUM(ri.quantity_purchased) AS total_items
FROM receipts r
JOIN receipt_items ri ON r.receipt_id = ri.receipt_id
WHERE r.rewards_receipt_status IN ('Accepted', 'Rejected')
GROUP BY r.rewards_receipt_status;
```

## Q5. Brand with the most spend among users created within the past 6 months

```sql
SELECT b.name, SUM(ri.final_price) AS total_spend
FROM users u
JOIN receipts r ON u.user_id = r.user_id
JOIN receipt_items ri ON r.receipt_id = ri.receipt_id
JOIN brands b ON ri.brand_code = b.brand_code
WHERE u.created_date >= current_date - interval '6 months'
GROUP BY b.name
ORDER BY total_spend DESC
LIMIT 1;
```

## Q6. Brand with the most transactions among users created within the past 6 months

```sql
SELECT b.name, COUNT(ri.receipt_item_id) AS transaction_count
FROM users u
JOIN receipts r ON u.user_id = r.user_id
JOIN receipt_items ri ON r.receipt_id = ri.receipt_id
JOIN brands b ON ri.brand_code = b.brand_code
WHERE u.created_date >= current_date - interval '6 months'
GROUP BY b.name
ORDER BY transaction_count DESC
LIMIT 1;
```

# Executing Queries Using Docker-Compose

1. To streamline the process, I transformed the unstructured data into a relational database format.
2. The provided docker-compose file preloads this data into a PostgreSQL database and offers an interactive interface for executing queries.
3. I've uploaded the Docker image to Docker Hub. You can download and access the docker-compose file in this directory with the following command:

   ```shell
   docker compose up --build
   ```

4. Once the setup is complete, navigate to [localhost:8082](http://localhost:8082/) to execute the below queries as needed.
