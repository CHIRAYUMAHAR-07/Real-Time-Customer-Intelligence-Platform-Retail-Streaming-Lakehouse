fig, axes = plt.subplots(1, 2, figsize=(18, 7), facecolor=DARK_BG)
fig.suptitle('  VIZ 14–15: RFM Analysis & Customer Value Matrix',
             fontsize=16, fontweight='bold', color=WHITE, y=1.02)

ax14 = axes[0]
ax14.set_facecolor(CARD_BG)
rfm_heat = df_gold.groupby(['r_score','f_score'])['monetary_total'].mean().unstack(fill_value=0)
sns.heatmap(rfm_heat, ax=ax14, cmap='YlOrRd',
            annot=True, fmt='.0f',
            cbar_kws={'label': 'Avg Monetary ($)'},
            linewidths=0.5, linecolor=DARK_BG)
ax14.set_xlabel('Frequency Score (1=Low → 5=High)', color=GRAY)
ax14.set_ylabel('Recency Score (1=Old → 5=Recent)', color=GRAY)
ax14.set_title('RFM Value Heatmap\n(R-Score × F-Score → Avg Revenue)', color=WHITE,
               fontsize=12, fontweight='bold')

ax15 = axes[1]
ax15.set_facecolor(CARD_BG)
tier_rev = rfm_model.groupby('clv_tier', observed=True)['clv_12m'].sum()
wedge_colors = [YELLOW, GREEN, ORANGE, RED]
wedges, texts, autotexts = ax15.pie(
    tier_rev.values,
    labels      = [str(t) for t in tier_rev.index],
    colors      = wedge_colors[:len(tier_rev)],
    autopct     = '%1.1f%%',
    startangle  = 90,
    pctdistance = 0.75,
    wedgeprops  = {'edgecolor': DARK_BG, 'linewidth': 2, 'width': 0.6}
)
for text in texts + autotexts:
    text.set_color(WHITE)
    text.set_fontsize(10)
ax15.set_title('12-Month Revenue by CLV Tier\n(BG/NBD model)', color=WHITE,
               fontsize=12, fontweight='bold')

plt.tight_layout()
plt.savefig('/content/viz_14_15_rfm.png', dpi=150,
            bbox_inches='tight', facecolor=DARK_BG)
plt.show()
print(' VIZ 14, 15 saved')