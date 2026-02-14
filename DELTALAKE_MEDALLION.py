fig, axes = plt.subplots(1, 2, figsize=(16, 6), facecolor=DARK_BG)
fig.suptitle('🏔️  VIZ 4–5: Delta Lake Medallion Layer Stats', fontsize=16,
             fontweight='bold', color=WHITE, y=1.02)

av4 = axes[0]
layers    = ['Kafka\n(Raw)', 'Bronze\n(Raw)', 'Silver\n(Cleaned)', 'Gold\n(Features)']
counts    = [len(df_events)*10, len(df_events), len(df_silver), len(df_gold)]
colors_fn = ['#8B4513', '#CD853F', '#C0C0C0', '#FFD700']
bars = av4.barh(layers, counts, color=colors_fn, edgecolor=DARK_BG)
for bar, cnt in zip(bars, counts):
    av4.text(bar.get_width() + 5000, bar.get_y() + bar.get_height()/2,
             f'{cnt:,}', va='center', fontsize=10, color=WHITE)
av4.set_title('Medallion Pipeline Funnel\n(Row counts per layer)', color=WHITE,
              fontsize=12, fontweight='bold')
av4.set_xlabel('Rows', color=GRAY)
av4.invert_yaxis()

av5 = axes[1]
cat_rev = df_mat.groupby('category')['total_revenue'].sum().sort_values(ascending=True)
colors_cat = plt.cm.RdYlGn(np.linspace(0.2, 0.9, len(cat_rev)))
av5.barh(cat_rev.index, cat_rev.values, color=colors_cat)
for i, v in enumerate(cat_rev.values):
    av5.text(v + 10, i, f'${v:,.0f}', va='center', fontsize=9, color=WHITE)
av5.set_title('Total Revenue by Category\n(Materialized View)', color=WHITE,
              fontsize=12, fontweight='bold')
av5.set_xlabel('Revenue ($)', color=GRAY)

plt.tight_layout()
plt.savefig('/content/viz_4_5_delta_lake.png', dpi=150,
            bbox_inches='tight', facecolor=DARK_BG)
plt.show()
print(' VIZ 4, 5 saved')