fig = plt.figure(figsize=(20, 7), facecolor=DARK_BG)
fig.suptitle('💰  VIZ 7–9: BG/NBD Customer Lifetime Value Model',
             fontsize=16, fontweight='bold', color=WHITE, y=1.02)

ax7 = fig.add_subplot(1, 3, 1)
ax7.set_facecolor(CARD_BG)
clv_vals = rfm_model[rfm_model['clv_12m'] < rfm_model['clv_12m'].quantile(0.99)]['clv_12m']
ax7.hist(clv_vals, bins=50, color=YELLOW, alpha=0.85, edgecolor=DARK_BG)
ax7.axvline(clv_vals.mean(),   color=RED,    lw=2, ls='--', label=f'Mean: ${clv_vals.mean():.0f}')
ax7.axvline(clv_vals.median(), color=GREEN,  lw=2, ls='--', label=f'Median: ${clv_vals.median():.0f}')
ax7.set_title('12-Month CLV Distribution', color=WHITE, fontsize=12, fontweight='bold')
ax7.set_xlabel('CLV ($)', color=GRAY)
ax7.legend(facecolor=CARD_BG, labelcolor=WHITE, fontsize=9)

ax8 = fig.add_subplot(1, 3, 2)
ax8.set_facecolor(CARD_BG)
rfm_sorted = rfm_model.sort_values('clv_12m', ascending=False)
total_rev   = rfm_sorted['clv_12m'].sum()
cumrev      = rfm_sorted['clv_12m'].cumsum() / total_rev * 100
cumpct      = np.arange(1, len(rfm_sorted)+1) / len(rfm_sorted) * 100

ax8.plot(cumpct, cumrev, color=ACCENT, lw=2.5)
ax8.fill_between(cumpct, cumrev, alpha=0.12, color=ACCENT)
ax8.axvline(20, color=YELLOW, lw=2, ls='--')
ax8.axhline(top20_pct*100, color=ORANGE, lw=2, ls='--')
ax8.annotate(f'Top 20% → {top20_pct:.0%} revenue',
             xy=(20, top20_pct*100),
             xytext=(35, top20_pct*100 - 8),
             color=WHITE, fontsize=10, fontweight='bold',
             arrowprops=dict(arrowstyle='->', color=ORANGE))
ax8.set_xlabel('% of Customers', color=GRAY)
ax8.set_ylabel('% of Revenue (Cumulative)', color=GRAY)
ax8.set_title(f'Pareto: Top 20% = {top20_pct:.0%} Revenue', color=WHITE, fontsize=12, fontweight='bold')

ax9 = fig.add_subplot(1, 3, 3)
ax9.set_facecolor(CARD_BG)
sc = ax9.scatter(
    rfm_model['recency'].sample(3000, random_state=42),
    rfm_model['frequency'].sample(3000, random_state=42),
    c  = rfm_model['prob_alive'].sample(3000, random_state=42),
    cmap='RdYlGn', alpha=0.5, s=20,
    vmin=0, vmax=1
)
plt.colorbar(sc, ax=ax9, label='P(alive)')
ax9.set_xlabel('Recency (days)', color=GRAY)
ax9.set_ylabel('Frequency (repeat purchases)', color=GRAY)
ax9.set_title('Probability Alive\n(Recency vs Frequency)', color=WHITE, fontsize=12, fontweight='bold')

plt.tight_layout()
plt.savefig('/content/viz_7_8_9_clv_model.png', dpi=150,
            bbox_inches='tight', facecolor=DARK_BG)
plt.show()
print(' VIZ 7, 8, 9 saved')