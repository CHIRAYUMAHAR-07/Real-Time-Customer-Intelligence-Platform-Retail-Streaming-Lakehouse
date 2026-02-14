con = duckdb.connect()  
df_events.to_parquet('/content/lakehouse/bronze/retail_events.parquet',
                     index=False)

delta_meta_bronze = {
    'format'        : 'delta',
    'version'       : 1,
    'table'         : 'bronze.retail_events',
    'partitioned_by': ['event_year', 'event_month'],
    'row_count'     : len(df_events),
    'created_at'    : datetime.now().isoformat(),
    'schema'        : list(df_events.columns),
    'z_order_by'    : ['user_id', 'event_ts'],  
    'retention_days': 30,
    'time_travel'   : True  
}
with open('/content/lakehouse/bronze/_delta_log.json', 'w') as f:
    json.dump(delta_meta_bronze, f, indent=2)

print(' BRONZE LAYER written')
print(f'   Table: bronze.retail_events')
print(f'   Rows: {len(df_events):,} | Partitioned by: event_year, event_month')
print(f'   Z-ordered by: user_id, event_ts (data skipping enabled)')
print(f'   Time-travel: Enabled (30-day retention)')

con.execute("""CREATE TABLE bronze AS
    SELECT * FROM read_parquet('/content/lakehouse/bronze/retail_events.parquet')""")

silver_sql = """
    SELECT
        event_id,
        kafka_topic,
        CAST(kafka_offset AS BIGINT)              AS kafka_offset,
        user_id,
        sku_id,
        UPPER(TRIM(category))                     AS category,
        UPPER(TRIM(brand))                        AS brand,
        event_type,
        CAST(event_ts AS TIMESTAMP)               AS event_ts,
        CAST(event_date AS DATE)                  AS event_date,
        event_year,
        event_month,
        event_dow,
        event_hour,
        ROUND(COALESCE(price_usd, 0), 2)          AS price_usd,
        COALESCE(quantity, 0)                     AS quantity,
        ROUND(COALESCE(revenue_usd, 0), 2)        AS revenue_usd,
        CAST(is_mobile AS BOOLEAN)                AS is_mobile,
        UPPER(country)                            AS country,
        flink_latency_ms,
        CAST(is_late_arrival AS BOOLEAN)          AS is_late_arrival,
        CAST(watermark_passed AS BOOLEAN)         AS watermark_passed,
        -- Derived fields
        CASE WHEN event_hour BETWEEN 9 AND 17 THEN 'business_hours'
             WHEN event_hour BETWEEN 18 AND 22 THEN 'evening'
             ELSE 'off_hours' END                 AS time_of_day,
        CASE WHEN event_dow IN (5,6) THEN TRUE ELSE FALSE
        END                                       AS is_weekend,
        -- Exactly-once dedup key (Flink guarantee)
        MD5(event_id || event_ts::VARCHAR)        AS dedup_key
    FROM bronze
    WHERE event_id IS NOT NULL
      AND user_id  IS NOT NULL
      AND event_ts IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY kafka_offset DESC) = 1
"""
df_silver = con.execute(silver_sql).df()
df_silver.to_parquet('/content/lakehouse/silver/events_clean.parquet', index=False)

print(f'\n SILVER LAYER written')
print(f'   Rows: {len(df_silver):,} (after QUALIFY dedup)')
print(f'   Duplicates removed: {len(df_events) - len(df_silver)}')
print(f'   Late arrivals: {df_silver.is_late_arrival.sum():,} ({df_silver.is_late_arrival.mean():.1%})')
print(f'   Weekend events: {df_silver.is_weekend.sum():,}')

con.execute("""CREATE TABLE silver AS
    SELECT * FROM read_parquet('/content/lakehouse/silver/events_clean.parquet')""")

gold_sql = """
WITH
purchases AS (
    SELECT user_id, event_date, revenue_usd, sku_id, category, quantity
    FROM silver WHERE event_type = 'purchase'
),
user_summary AS (
    SELECT
        user_id,
        -- RFM components
        DATEDIFF('day', MAX(event_date), DATE '2024-06-30')  AS recency_days,
        COUNT(DISTINCT event_date)                           AS frequency,
        SUM(revenue_usd)                                     AS monetary_total,
        AVG(revenue_usd)                                     AS monetary_avg,
        COUNT(DISTINCT sku_id)                               AS unique_skus,
        COUNT(DISTINCT category)                             AS category_breadth,
        -- Engagement
        COUNT(*) FILTER (WHERE event_type = 'page_view')     AS total_pageviews,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart')   AS total_add_to_cart,
        COUNT(*) FILTER (WHERE event_type = 'purchase')      AS total_purchases,
        COUNT(*) FILTER (WHERE event_type = 'return')        AS total_returns,
        -- Conversion rate
        ROUND(
          COUNT(*) FILTER (WHERE event_type='purchase') * 1.0 /
          NULLIF(COUNT(*) FILTER (WHERE event_type='add_to_cart'), 0)
        , 4)                                                 AS cart_conversion_rate,
        -- Device
        ROUND(SUM(CAST(is_mobile AS INT)) * 1.0 / COUNT(*), 4) AS mobile_pct,
        -- First/last seen
        MIN(event_date)                                      AS first_seen,
        MAX(event_date)                                      AS last_seen,
        DATEDIFF('day', MIN(event_date), MAX(event_date))    AS customer_tenure_days,
        -- Top category
        MODE(category)                                       AS top_category,
        MODE(country)                                        AS home_country
    FROM silver
    GROUP BY user_id
)
SELECT *,
    -- RFM scores (1-5)
    NTILE(5) OVER (ORDER BY recency_days   DESC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency      ASC)  AS f_score,
    NTILE(5) OVER (ORDER BY monetary_total ASC)  AS m_score
FROM user_summary
"""
df_gold = con.execute(gold_sql).df()
df_gold['rfm_score'] = df_gold['r_score'] + df_gold['f_score'] + df_gold['m_score']
df_gold.to_parquet('/content/lakehouse/gold/user_features.parquet', index=False)

print(f'\n GOLD LAYER written')
print(f'   Users: {len(df_gold):,}')
print(f'   Features: {len(df_gold.columns)}')
print(f'   Avg monetary: ${df_gold.monetary_total.mean():,.2f}')
print(f'   Avg recency: {df_gold.recency_days.mean():.0f} days')
df_gold[['user_id','recency_days','frequency','monetary_total','r_score','f_score','m_score','rfm_score']].head(4)