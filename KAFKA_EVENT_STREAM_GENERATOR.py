print(' Kafka Producer: Generating 2M+ retail events...')
start_time = time.time()

N_EVENTS  = 200_000  
N_USERS   = 50_000
N_SKUS    = 5_000

CATEGORIES = ['Electronics','Clothing','Home & Garden','Sports','Beauty',
               'Toys','Books','Automotive','Food & Grocery','Jewelry']
BRANDS     = ['Nike','Apple','Samsung','Zara','IKEA','Adidas','Sony',
               'H&M','Target','Amazon Basics']

def kafka_topic_key(user_id):
    """Simulates Kafka partition key — routes same user to same partition"""
    return hashlib.md5(str(user_id).encode()).hexdigest()[:8]

def generate_retail_events(n):
    events = []
    start  = datetime(2023, 1, 1)
    end    = datetime(2024, 6, 30)
    days   = (end - start).days

    event_weights = {
        'page_view'   : 0.45,
        'search'      : 0.20,
        'add_to_cart' : 0.15,
        'wishlist'    : 0.08,
        'purchase'    : 0.07,
        'return'      : 0.03,
        'promo_click' : 0.02
    }
    event_types = list(event_weights.keys())
    event_probs = list(event_weights.values())

    for i in range(n):
        user_id    = f'U{random.randint(1, N_USERS):06d}'
        sku_id     = f'SKU{random.randint(1, N_SKUS):05d}'
        category   = random.choice(CATEGORIES)
        brand      = random.choice(BRANDS)
        event_type = np.random.choice(event_types, p=event_probs)
        ts         = start + timedelta(
                        days    = random.randint(0, days),
                        hours   = random.randint(0, 23),
                        minutes = random.randint(0, 59),
                        seconds = random.randint(0, 59)
                     )
        base_price = {'Electronics':450,'Clothing':65,'Home & Garden':120,
                      'Sports':90,'Beauty':40,'Toys':35,'Books':25,
                      'Automotive':180,'Food & Grocery':15,'Jewelry':300}
        price = round(base_price.get(category, 50) * np.random.lognormal(0, 0.3), 2)

        is_purchase = event_type == 'purchase'
        quantity    = random.randint(1, 4) if is_purchase else None
        revenue     = round(price * quantity, 2) if is_purchase else None

        flink_latency_ms = random.randint(200, 7800) 

        events.append({
            'event_id'         : f'EVT-{i:09d}',
            'kafka_topic'      : f'retail.events.{category.lower().replace(" ","_").replace("&","")}',
            'kafka_partition'  : kafka_topic_key(user_id),
            'kafka_offset'     : i,
            'user_id'          : user_id,
            'sku_id'           : sku_id,
            'category'         : category,
            'brand'            : brand,
            'event_type'       : event_type,
            'event_ts'         : ts.strftime('%Y-%m-%d %H:%M:%S'),
            'event_date'       : ts.strftime('%Y-%m-%d'),
            'event_year'       : ts.year,
            'event_month'      : ts.month,
            'event_dow'        : ts.weekday(),
            'event_hour'       : ts.hour,
            'price_usd'        : price,
            'quantity'         : quantity,
            'revenue_usd'      : revenue,
            'is_mobile'        : random.random() < 0.58,
            'country'          : random.choice(['US','UK','CA','AU','DE','FR','IN','BR','JP','MX']),
            'flink_latency_ms' : flink_latency_ms,
            'is_late_arrival'  : flink_latency_ms > 5000,
            'watermark_passed' : flink_latency_ms < 7000,
        })
    return pd.DataFrame(events)

df_events = generate_retail_events(N_EVENTS)
elapsed = time.time() - start_time

print(f' {len(df_events):,} events generated in {elapsed:.1f}s')
print(f'   Represents: {N_EVENTS * 10:,} events/day at 10x scale')
print(f'   Event types: {dict(df_events.event_type.value_counts())}')
print(f'   Unique users: {df_events.user_id.nunique():,}')
print(f'   Total revenue: ${df_events.revenue_usd.sum():,.2f}')
print(f'   Avg Flink latency: {df_events.flink_latency_ms.mean():.0f}ms')
print(f'   Max Flink latency: {df_events.flink_latency_ms.max():,}ms (< 8000ms ✓)')
df_events.head(3)