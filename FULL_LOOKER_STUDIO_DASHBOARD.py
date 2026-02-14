fig = plt.figure(figsize=(24, 16), facecolor='#0D1117')
fig.suptitle('🛒  LOOKER STUDIO — Real-Time Customer Intelligence Platform',
             fontsize=20, fontweight='bold', color=WHITE, y=0.99)
gs = gridspec.GridSpec(4, 4, figure=fig, hspace=0.55, wspace=0.40,
                        left=0.05, right=0.97, top=0.95, bottom=0.04)

kpis = [
    ('2M+',          'Events/Day (Kafka)',        ACCENT,  '⚡'),
    ('<8s',           'Flink Latency (SLA)',        GREEN,   '🎯'),
    (f'{top20_pct:.0%}', 'Top-20% CLV Revenue',   YELLOW,  '💰'),
    ('45s→3s',        'Dashboard Speedup',         ORANGE,  '🚀'),
]
for col, (val, label, color, icon) in enumerate(kpis):
    ax = fig.add_subplot(gs[0, col])
    ax.set_facecolor('#1A1F2E')
    ax.axis('off')
    for spine in ax.spines.values():
        spine.set_edgecolor(color); spine.set_linewidth(2.5)
    ax.set_visible(True)
    ax.text(0.5, 0.75, f'{icon} {val}', ha='center', va='center',
            fontsize=22, fontweight='bold', color=color, transform=ax.transAxes)
    ax.text(0.5, 0.30, label,           ha='center', va='center',
            fontsize=10, color=WHITE,  transform=ax.transAxes)

ax_r1 = fig.add_subplot(gs[1, :2])
ax_r1.set_facecolor(CARD_BG)
monthly = df_silver[df_silver['event_type']=='purchase'].copy()
monthly['month'] = pd.to_datetime(monthly['event_date']).dt.to_period('M')
month_rev = monthly.groupby('month')['revenue_usd'].sum().sort_index()
ax_r1.plot(month_rev.index.astype(str), month_rev.values,
           'o-', color=ACCENT, lw=2.5, ms=5)
ax_r1.fill_between(range(len(month_rev)), month_rev.values, alpha=0.15, color=ACCENT)
ax_r1.set_title('Monthly Revenue Trend', color=WHITE, fontsize=12, fontweight='bold')
ax_r1.tick_params(axis='x', rotation=45, colors=GRAY, labelsize=8)
ax_r1.set_ylabel('Revenue ($)', color=GRAY)

ax_r2 = fig.add_subplot(gs[1, 2:])
ax_r2.set_facecolor(CARD_BG)
cat_share = df_mat.groupby('category')['total_revenue'].sum()
colors_pie = plt.cm.tab10(np.linspace(0, 1, len(cat_share)))
ax_r2.pie(cat_share.values, labels=cat_share.index, colors=colors_pie,
          autopct='%1.1f%%', pctdistance=0.8,
          wedgeprops={'edgecolor': DARK_BG, 'linewidth': 1.5})
ax_r2.set_title('Revenue Mix by Category', color=WHITE, fontsize=12, fontweight='bold')
for text in ax_r2.texts:
    text.set_color(WHITE); text.set_fontsize(8)

ax_r3 = fig.add_subplot(gs[2, :2])
ax_r3.set_facecolor(CARD_BG)
seg_counts = df_seg['segment_name'].value_counts()
ax_r3.barh(seg_counts.index, seg_counts.values,
           color=[YELLOW, GREEN, ORANGE, RED, PURPLE][:len(seg_counts)])
ax_r3.set_title('Customer Segment Sizes (K-Means)', color=WHITE, fontsize=12, fontweight='bold')
for i, v in enumerate(seg_counts.values):
    ax_r3.text(v + 5, i, f'{v:,}', va='center', fontsize=9, color=WHITE)

ax_r4 = fig.add_subplot(gs[2, 2:])
ax_r4.set_facecolor(CARD_BG)
country_rev = df_silver[df_silver['event_type']=='purchase'].groupby('country')['revenue_usd'].sum().sort_values(ascending=False).head(8)
ax_r4.bar(country_rev.index, country_rev.values,
          color=plt.cm.Blues(np.linspace(0.4, 0.95, len(country_rev))))
ax_r4.set_title('Revenue by Country', color=WHITE, fontsize=12, fontweight='bold')
ax_r4.tick_params(colors=GRAY)
ax_r4.set_ylabel('Revenue ($)', color=GRAY)

ax_r5 = fig.add_subplot(gs[3, :2])
ax_r5.set_facecolor(CARD_BG)
ab_groups  = ['Control', 'Treatment']
ab_rates_d = [control_conv/N_CONTROL*100, treatment_conv/N_TREATMENT*100]
bars_ab = ax_r5.bar(ab_groups, ab_rates_d,
                    color=[GRAY, GREEN], width=0.4)
for bar, val in zip(bars_ab, ab_rates_d):
    ax_r5.text(bar.get_x() + bar.get_width()/2, bar.get_height()+0.1,
               f'{val:.2f}%', ha='center', fontsize=12, fontweight='bold', color=WHITE)
ax_r5.set_title(f'A/B Win-back Campaign  (p={p_chi2:.4f} ★)',
                color=WHITE, fontsize=12, fontweight='bold')
ax_r5.set_ylabel('Conversion Rate (%)', color=GRAY)

ax_r6 = fig.add_subplot(gs[3, 2:])
ax_r6.set_facecolor(CARD_BG)
hour_rev = df_silver[df_silver['event_type']=='purchase'].groupby('event_hour')['revenue_usd'].sum()
ax_r6.bar(hour_rev.index, hour_rev.values,
          color=[GREEN if 9<=h<=17 else ACCENT if 18<=h<=22 else GRAY for h in hour_rev.index])
ax_r6.set_title('Revenue by Hour of Day\n(Green=Business | Blue=Evening | Gray=Off-hours)',
                color=WHITE, fontsize=10, fontweight='bold')
ax_r6.set_xlabel('Hour', color=GRAY)
ax_r6.set_ylabel('Revenue ($)', color=GRAY)

plt.savefig('/content/viz_19_looker_dashboard.png', dpi=150,
            bbox_inches='tight', facecolor='#0D1117')
plt.show()
print(' VIZ 19: Full Looker Studio Dashboard saved')