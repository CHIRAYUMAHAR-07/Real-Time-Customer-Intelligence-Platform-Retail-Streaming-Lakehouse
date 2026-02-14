print(' dbt Models:')
print()

dbt_models = {
    'dbt_project.yml': """
name: 'retail_streaming_lakehouse'
version: '2.0.0'
profile: 'bigquery_prod'

model-paths: ['models']
test-paths:  ['tests']
docs-paths:  ['docs']

models:
  retail_streaming_lakehouse:
    bronze:
      +materialized: view
      +tags: ['bronze','streaming']
    silver:
      +materialized: incremental
      +unique_key: 'dedup_key'
      +partition_by:
        field: event_date
        data_type: date
      +cluster_by: ['user_id', 'category']
      +tags: ['silver']
    gold:
      +materialized: table
      +tags: ['gold','serving']
    serving:
      +materialized: table
      +tags: ['serving','ml']
    """,

    'models/silver/stg_events.sql': """
-- Silver staging model: clean + deduplicate events
{{ config(
    materialized = 'incremental',
    unique_key   = 'dedup_key',
    partition_by = {'field': 'event_date', 'data_type': 'date'},
    cluster_by   = ['user_id', 'category']
) }}

SELECT
    event_id,
    user_id,
    sku_id,
    UPPER(TRIM(category))               AS category,
    event_type,
    CAST(event_ts AS TIMESTAMP)         AS event_ts,
    CAST(event_date AS DATE)            AS event_date,
    ROUND(COALESCE(revenue_usd, 0), 2)  AS revenue_usd,
    CAST(is_mobile AS BOOL)             AS is_mobile,
    country,
    flink_latency_ms,
    MD5(event_id || CAST(event_ts AS STRING)) AS dedup_key
FROM {{ source('bronze', 'retail_events') }}
{% if is_incremental() %}
    WHERE event_date > (SELECT MAX(event_date) FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id ORDER BY kafka_offset DESC
) = 1
    """,

    'models/gold/user_rfm.sql': """
-- Gold: RFM + CLV features for ML serving
{{ config(
    materialized = 'table',
    cluster_by   = ['clv_tier']
) }}

WITH rfm AS (
    SELECT
        user_id,
        DATE_DIFF(CURRENT_DATE,
            MAX(event_date), DAY)           AS recency_days,
        COUNT(DISTINCT event_date) - 1      AS frequency,
        AVG(revenue_usd)                    AS avg_order_value,
        SUM(revenue_usd)                    AS total_revenue,
        NTILE(5) OVER (ORDER BY SUM(revenue_usd) DESC) AS m_score
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'purchase'
    GROUP BY user_id
)
SELECT *,
    CASE
        WHEN m_score = 1 THEN 'Champions'
        WHEN m_score = 2 THEN 'Loyalists'
        WHEN m_score = 3 THEN 'At Risk'
        ELSE 'Churned'
    END AS clv_tier
FROM rfm
    """,

    'schema.yml': """
version: 2
models:
  - name: stg_events
    description: 'Silver layer: cleaned retail events'
    tests:
      - unique:
          column_name: dedup_key
      - not_null:
          column_name: user_id
      - not_null:
          column_name: event_ts
      - accepted_values:
          column_name: event_type
          values: [page_view, purchase, add_to_cart,
                   search, wishlist, return, promo_click]
      - dbt_expectations.expect_column_values_to_be_between:
          column_name: revenue_usd
          min_value: 0
          max_value: 50000
  - name: user_rfm
    description: 'Gold: RFM + CLV features'
    tests:
      - unique:
          column_name: user_id
      - not_null:
          column_name: clv_tier
    """
}

for filename, content in dbt_models.items():
    print(f' {filename}:')
    print(content[:300] + '...' if len(content) > 300 else content)
    print('-' * 60)

print(' dbt project structure defined')
print('   Production run: dbt run --select tag:silver+ --target prod')
print('   Tests: dbt test --select stg_events user_rfm')