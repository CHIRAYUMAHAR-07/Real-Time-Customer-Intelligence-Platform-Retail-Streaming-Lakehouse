print(' K-Means Segmentation on RFM + CLV features...')

df_seg = rfm_model[[
    'user_id', 'recency', 'frequency', 'monetary_value',
    'clv_12m', 'prob_alive', 'predicted_purchases_12m'
]].copy().fillna(0)

FEATURES = ['recency','frequency','monetary_value','clv_12m','prob_alive']
X = df_seg[FEATURES].values

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

inertias    = []
silhouettes = []
K_range     = range(2, 9)

for k in K_range:
    km  = KMeans(n_clusters=k, random_state=42, n_init=10)
    lbl = km.fit_predict(X_scaled)
    inertias.append(km.inertia_)
    silhouettes.append(silhouette_score(X_scaled, lbl, sample_size=2000))

optimal_k = K_range[np.argmax(silhouettes)]
print(f'   Optimal K = {optimal_k} (silhouette = {max(silhouettes):.4f})')

kmeans_final = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
df_seg['segment'] = kmeans_final.fit_predict(X_scaled)

seg_clv = df_seg.groupby('segment')['clv_12m'].mean()
seg_labels = {}
sorted_segs = seg_clv.sort_values(ascending=False)
names = [' VIP Champions', ' Loyal Customers', ' At-Risk Customers',
         ' Dormant Customers', ' New Customers']
for i, (seg, _) in enumerate(sorted_segs.items()):
    seg_labels[seg] = names[i] if i < len(names) else f'Segment {seg}'

df_seg['segment_name'] = df_seg['segment'].map(seg_labels)

stats = df_seg.groupby('segment_name').agg(
    count          = ('user_id',        'count'),
    avg_clv        = ('clv_12m',        'mean'),
    avg_recency    = ('recency',        'mean'),
    avg_frequency  = ('frequency',      'mean'),
    avg_monetary   = ('monetary_value', 'mean'),
    avg_prob_alive = ('prob_alive',     'mean')
).round(2)

print(f'\n K-Means Segments (K={optimal_k}):')
print(stats.to_string())

df_seg.to_parquet('/content/lakehouse/serving/customer_segments.parquet', index=False)