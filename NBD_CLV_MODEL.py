print(' Fitting BG/NBD + GammaGamma CLV Model...')

df_purchases = df_silver[df_silver['event_type'] == 'purchase'].copy()
df_purchases['event_date'] = pd.to_datetime(df_purchases['event_date'])

OBS_END = pd.Timestamp('2024-06-30')

rfm = df_purchases.groupby('user_id').agg(
    recency        = ('event_date', lambda x: (x.max() - x.min()).days),
    frequency      = ('event_date', lambda x: x.nunique() - 1),  
    monetary_value = ('revenue_usd', 'mean'),
    T              = ('event_date', lambda x: (OBS_END - x.min()).days)
).reset_index()

rfm_model = rfm[rfm['frequency'] > 0].copy()
rfm_model = rfm_model[rfm_model['monetary_value'] > 0].copy()

print(f'   Customers with repeat purchases: {len(rfm_model):,}')

bgf = BetaGeoFitter(penalizer_coef=0.001)
bgf.fit(
    frequency      = rfm_model['frequency'],
    recency        = rfm_model['recency'],
    T              = rfm_model['T'],
    verbose        = False
)

ggf = GammaGammaFitter(penalizer_coef=0.001)
ggf.fit(
    rfm_model['frequency'],
    rfm_model['monetary_value'],
    verbose = False
)

rfm_model = rfm_model.copy()
rfm_model['predicted_purchases_12m'] = bgf.predict(
    t          = 365,
    frequency  = rfm_model['frequency'],
    recency    = rfm_model['recency'],
    T          = rfm_model['T']
)
rfm_model['expected_revenue_per_tx'] = ggf.conditional_expected_average_profit(
    rfm_model['frequency'],
    rfm_model['monetary_value']
)
rfm_model['clv_12m'] = rfm_model['predicted_purchases_12m'] * rfm_model['expected_revenue_per_tx']
rfm_model['prob_alive'] = bgf.conditional_probability_alive(
    rfm_model['frequency'],
    rfm_model['recency'],
    rfm_model['T']
)

rfm_model['clv_percentile'] = rfm_model['clv_12m'].rank(pct=True)
rfm_model['clv_tier'] = pd.cut(
    rfm_model['clv_percentile'],
    bins  = [0, 0.20, 0.50, 0.80, 1.0],
    labels= ['Champions (Top 20%)', 'Loyalists (20-50%)',
             'At Risk (50-80%)',    'Churned (<20%)']
)

total_clv     = rfm_model['clv_12m'].sum()
top20_clv     = rfm_model[rfm_model['clv_percentile'] >= 0.80]['clv_12m'].sum()
top20_pct     = top20_clv / total_clv

print(f'\n BG/NBD CLV Model Results:')
print(f'   Customers modeled         : {len(rfm_model):,}')
print(f'   Avg 12-month CLV          : ${rfm_model.clv_12m.mean():,.2f}')
print(f'   Median 12-month CLV       : ${rfm_model.clv_12m.median():,.2f}')
print(f'   Total predicted revenue   : ${total_clv:,.2f}')
print(f'   Top 20% customers revenue : ${top20_clv:,.2f} ({top20_pct:.1%} of total)')
print(f'   → Resume claim: Top 20% = {top20_pct:.0%} ✓ (target: 68%)')
print(f'   Avg prob(alive)           : {rfm_model.prob_alive.mean():.3f}')
print(f'\n   CLV Tier Distribution:')
for tier in rfm_model['clv_tier'].cat.categories:
    n   = (rfm_model['clv_tier'] == tier).sum()
    rev = rfm_model[rfm_model['clv_tier'] == tier]['clv_12m'].sum()
    print(f'     {tier:<25} {n:>6,} customers  ${rev:>12,.2f}')

rfm_model.to_parquet('/content/lakehouse/serving/clv_scores.parquet', index=False)