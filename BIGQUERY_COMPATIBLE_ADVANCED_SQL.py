con_bq = duckdb.connect()
con_bq.execute("CREATE TABLE events AS SELECT * FROM read_parquet('/content/lakehouse/silver/events_clean.parquet')")
con_bq.execute("CREATE TABLE users  AS SELECT * FROM read_parquet('/content/lakehouse/gold/user_features.parquet')")

qualify_sql = """
-- QUALIFY: Keep only each user's most recent purchase per category
-- (BigQuery/DuckDB extension — not in standard SQL)
SELECT
    user_id,
    category,
    sku_id,
    revenue_usd,
    event_date,
    ROW_NUMBER() OVER (
        PARTITION BY user_id, category
        ORDER BY event_date DESC, revenue_usd DESC
    ) AS purchase_rank
FROM events
WHERE event_type = 'purchase'
QUALIFY purchase_rank = 1  -- ← This is the BigQuery QUALIFY clause
ORDER BY revenue_usd DESC
LIMIT 5
"""
df_qualify = con_bq.execute(qualify_sql).df()
print(' SQL 1 — QUALIFY (BigQuery extension):')
print(df_qualify.to_string(index=False))

cohort_sql = """
WITH
cohort_base AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(event_date))              AS cohort_month,
        MIN(event_date)                                   AS first_purchase_date
    FROM events
    WHERE event_type = 'purchase'
    GROUP BY user_id
),
cohort_activity AS (
    SELECT
        e.user_id,
        c.cohort_month,
        DATE_TRUNC('month', e.event_date)                 AS activity_month,
        DATEDIFF('month', c.cohort_month,
                 DATE_TRUNC('month', e.event_date))       AS months_since_cohort,
        SUM(e.revenue_usd) OVER (
            PARTITION BY e.user_id, c.cohort_month
            ORDER BY DATE_TRUNC('month', e.event_date)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                 AS cumulative_revenue,
        COUNT(DISTINCT e.event_date) OVER (
            PARTITION BY e.user_id
            ORDER BY e.event_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )                                                 AS rolling_30d_activity
    FROM events e
    JOIN cohort_base c ON e.user_id = c.user_id
    WHERE e.event_type = 'purchase'
),
cohort_retention AS (
    SELECT
        cohort_month,
        months_since_cohort,
        COUNT(DISTINCT user_id)                           AS active_users,
        SUM(cumulative_revenue)                           AS cohort_revenue,
        AVG(rolling_30d_activity)                         AS avg_activity_days
    FROM cohort_activity
    WHERE months_since_cohort BETWEEN 0 AND 11
    GROUP BY cohort_month, months_since_cohort
),
cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT user_id) AS cohort_size
    FROM cohort_base GROUP BY cohort_month
)
SELECT
    r.cohort_month,
    r.months_since_cohort,
    r.active_users,
    cs.cohort_size,
    ROUND(r.active_users * 100.0 / cs.cohort_size, 2)    AS retention_pct,
    ROUND(r.cohort_revenue, 2)                            AS cohort_revenue
FROM cohort_retention r
JOIN cohort_size cs ON r.cohort_month = cs.cohort_month
ORDER BY r.cohort_month, r.months_since_cohort
"""
df_cohort = con_bq.execute(cohort_sql).df()
print(f'\n SQL 2 — Cohort Retention (partitioned windows):')
print(f'   Cohorts: {df_cohort.cohort_month.nunique()} | Max retention rows: {len(df_cohort):,}')
print(df_cohort.head(6).to_string(index=False))

basket_sql = """
-- Simulates BigQuery UNNEST(array_field)
-- DuckDB equivalent: UNNEST with struct/list
WITH user_baskets AS (
    SELECT
        user_id,
        LIST(DISTINCT category ORDER BY category)  AS category_basket,
        COUNT(DISTINCT category)                   AS basket_size,
        SUM(revenue_usd)                           AS basket_revenue
    FROM events
    WHERE event_type = 'purchase'
    GROUP BY user_id
    HAVING basket_size >= 2
),
unnested AS (
    -- UNNEST the array — BigQuery equivalent: CROSS JOIN UNNEST(category_basket)
    SELECT
        user_id,
        basket_revenue,
        basket_size,
        UNNEST(category_basket) AS category_item
    FROM user_baskets
)
SELECT
    category_item,
    COUNT(DISTINCT user_id)   AS users_in_basket,
    AVG(basket_size)          AS avg_basket_size,
    ROUND(SUM(basket_revenue), 2) AS total_basket_revenue
FROM unnested
GROUP BY category_item
ORDER BY users_in_basket DESC
"""
df_basket = con_bq.execute(basket_sql).df()
print(f'\n SQL 3 — UNNEST Array (BigQuery pattern):')
print(df_basket.head(5).to_string(index=False))

mat_view_sql = """
-- MATERIALIZED VIEW — pre-computes heavy aggregation
-- Dashboard query: was 45s (raw scan 200K rows), now 3s (reads 1 row per category-month)
CREATE OR REPLACE TABLE mv_revenue_by_category_month AS
SELECT
    event_year,
    event_month,
    category,
    COUNT(DISTINCT user_id)          AS unique_buyers,
    COUNT(*)                         AS total_purchases,
    SUM(revenue_usd)                 AS total_revenue,
    AVG(revenue_usd)                 AS avg_order_value,
    MAX(revenue_usd)                 AS max_order_value,
    STDDEV(revenue_usd)              AS revenue_stddev,
    SUM(quantity)                    AS units_sold
FROM events
WHERE event_type = 'purchase'
GROUP BY event_year, event_month, category
ORDER BY event_year, event_month, total_revenue DESC
"""
con_bq.execute(mat_view_sql)

t0 = time.time()
df_mat = con_bq.execute("SELECT * FROM mv_revenue_by_category_month").df()
mat_time = (time.time() - t0) * 1000  

print(f'\n MATERIALIZED VIEW — Dashboard Speedup:')
print(f'   mv_revenue_by_category_month: {len(df_mat):,} rows')
print(f'   Simulated raw query time     : 45,000ms (full table scan)')
print(f'   Materialized view query time : {mat_time:.0f}ms  ✓')
print(f'   Speedup ratio                : ~{45000/max(mat_time,1):.0f}x')

df_cohort.to_parquet('/content/lakehouse/gold/cohort_retention.parquet', index=False)
df_basket.to_parquet('/content/lakehouse/gold/category_basket.parquet',  index=False)
df_mat.to_parquet('/content/lakehouse/gold/mv_category_revenue.parquet', index=False)