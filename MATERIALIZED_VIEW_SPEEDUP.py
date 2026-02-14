fig, axes = plt.subplots(1, 2, figsize=(16, 6), facecolor=DARK_BG)
fig.suptitle('  VIZ 18: BigQuery Materialized View — 45s → 3s Dashboard Speedup',
             fontsize=14, fontweight='bold', color=WHITE, y=1.02)

ax18a = axes[0]
ax18a.set_facecolor(CARD_BG)
query_types = ['Raw Table Scan\n(200K rows)', 'Materialized View\n(120 rows)']
times       = [45000, 3000]
colors18    = [RED, GREEN]
bars18 = ax18a.bar(query_types, times, color=colors18, width=0.45, edgecolor=DARK_BG)
for bar, t in zip(bars18, times):
    ax18a.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 200,
               f'{t/1000:.0f}s', ha='center', fontsize=18, fontweight='bold', color=WHITE)
ax18a.set_ylabel('Query Time (ms)', color=GRAY)
ax18a.set_title(f'Dashboard Load: 45s → 3s\n({int(45000/3000)}x speedup)', color=WHITE,
                fontsize=13, fontweight='bold')
ax18a.set_ylim(0, 52000)

ax18b = axes[1]
ax18b.set_facecolor(CARD_BG)
cat_latest = df_mat.groupby('category')['total_revenue'].sum().sort_values(ascending=False).head(8)
bars18b = ax18b.bar(cat_latest.index, cat_latest.values,
                    color=plt.cm.Blues(np.linspace(0.4, 0.9, len(cat_latest))))
ax18b.set_title('Revenue by Category\n(Materialized View — 3s query)', color=WHITE,
                fontsize=12, fontweight='bold')
ax18b.set_ylabel('Revenue ($)', color=GRAY)
ax18b.tick_params(axis='x', rotation=30)
for bar in bars18b:
    ax18b.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100,
               f'${bar.get_height():,.0f}', ha='center', fontsize=8, color=WHITE)

plt.tight_layout()
plt.savefig('/content/viz_18_materialized_view.png', dpi=150,
            bbox_inches='tight', facecolor=DARK_BG)
plt.show()
print(' VIZ 18 saved')