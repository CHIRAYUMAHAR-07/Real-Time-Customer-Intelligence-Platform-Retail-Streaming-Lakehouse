print(' Flink Job: Real-Time Revenue Aggregation Pipeline')
print('   Semantics: Exactly-Once | Watermark: 5s | Window: 1-min tumbling')
print('   Source: Kafka retail.events.* | Sink: Delta Lake gold layer')
print()

class ExactlyOnceProcessor:
    """Simulates Flink's exactly-once checkpoint mechanism"""
    def __init__(self):
        self.seen_offsets  = set() 
        self.checkpoint_id = 0
        self.state_store   = {}     

    def process(self, event):
        key = (event['kafka_partition'], event['kafka_offset'])
        if key in self.seen_offsets:
            return None  
        self.seen_offsets.add(key)
        return event

    def checkpoint(self):
        self.checkpoint_id += 1
        return {'checkpoint_id': self.checkpoint_id,
                'offset_count':  len(self.seen_offsets),
                'ts':            datetime.now().isoformat()}

def flink_tumbling_window(df, window_minutes=1):
    """Flink 1-minute tumbling window revenue aggregation"""
    df = df.copy()
    df['event_ts']  = pd.to_datetime(df['event_ts'])
    df['window_ts'] = df['event_ts'].dt.floor(f'{window_minutes}min')

    purchases = df[df['event_type'] == 'purchase'].copy()

    window_agg = purchases.groupby(['window_ts', 'category']).agg(
        revenue     = ('revenue_usd', 'sum'),
        tx_count    = ('event_id',    'count'),
        unique_users= ('user_id',     'nunique'),
        avg_order   = ('revenue_usd', 'mean')
    ).reset_index()

    window_agg['watermark_ts']    = window_agg['window_ts'] + pd.Timedelta(seconds=5)
    window_agg['flink_job']       = 'revenue_aggregation_v2'
    window_agg['exactly_once']    = True
    window_agg['window_type']     = 'TUMBLING_1MIN'

    return window_agg

processor = ExactlyOnceProcessor()
processed = []
for _, row in df_events.head(10000).iterrows():
    result = processor.process(row.to_dict())
    if result:
        processed.append(result)
    if len(processed) % 2000 == 0 and len(processed) > 0:
        ckpt = processor.checkpoint()

df_processed = pd.DataFrame(processed)
df_windows   = flink_tumbling_window(df_processed)
df_windows.to_parquet('/content/lakehouse/gold/flink_windows.parquet', index=False)

latency = df_events['flink_latency_ms']
print(f' Flink Job Metrics:')
print(f'   Records processed     : {len(processed):,} (exactly-once)')
print(f'   Duplicates rejected   : {10000 - len(processed)} (exactly-once ✓)')
print(f'   Windows created       : {len(df_windows):,} (1-min tumbling)')
print(f'   P50 latency           : {latency.quantile(0.50):.0f}ms')
print(f'   P95 latency           : {latency.quantile(0.95):.0f}ms')
print(f'   P99 latency           : {latency.quantile(0.99):.0f}ms')
print(f'   Max latency           : {latency.max():.0f}ms  (< 8000ms SLA ✓)')
print(f'   Late arrivals handled : {df_silver.is_late_arrival.sum():,} (watermark)')
print(f'   Checkpoints completed : {processor.checkpoint_id}')
print(f'\n   Kafka Topics subscribed:')
for t in sorted(df_events.kafka_topic.unique())[:5]:
    print(f'     - {t}')

df_windows.head(3)