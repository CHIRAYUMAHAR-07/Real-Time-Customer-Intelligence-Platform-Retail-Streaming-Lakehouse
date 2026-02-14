fig, axes = plt.subplots(1, 2, figsize=(18, 7), facecolor=DARK_BG)
fig.suptitle('  VIZ 10–11: K-Means Customer Segmentation',
             fontsize=16, fontweight='bold', color=WHITE, y=1.02)

ax10 = axes[0]
ax10.set_facecolor(CARD_BG)
ax10b = ax10.twinx()
ax10.plot(list(K_range), inertias,    'o-', color=ACCENT,  lw=2.5, label='Inertia')
ax10b.plot(list(K_range), silhouettes,'s-', color=GREEN,   lw=2.5, label='Silhouette')
ax10.axvline(optimal_k, color=YELLOW, lw=2, ls='--', label=f'Optimal K={optimal_k}')
ax10.set_xlabel('Number of Clusters (K)', color=GRAY)
ax10.set_ylabel('Inertia', color=ACCENT)
ax10b.set_ylabel('Silhouette Score', color=GREEN)
ax10.set_title(f'Elbow + Silhouette Method\nOptimal K = {optimal_k}',
               color=WHITE, fontsize=12, fontweight='bold')
lines1, labels1 = ax10.get_legend_handles_labels()
lines2, labels2 = ax10b.get_legend_handles_labels()
ax10.legend(lines1 + lines2, labels1 + labels2,
            facecolor=CARD_BG, labelcolor=WHITE, fontsize=9)

ax11 = axes[1]
ax11.set_facecolor(CARD_BG)
seg_summary = df_seg.groupby('segment_name').agg(
    avg_clv      = ('clv_12m',        'mean'),
    avg_freq     = ('frequency',      'mean'),
    count        = ('user_id',        'count'),
    avg_prob     = ('prob_alive',     'mean')
).reset_index()

cols_seg = [YELLOW, GREEN, ORANGE, RED, PURPLE]
for i, row in seg_summary.iterrows():
    ax11.scatter(row['avg_freq'], row['avg_clv'],
                 s     = row['count'] / 3,
                 color = cols_seg[i % len(cols_seg)],
                 alpha = 0.75, edgecolors='white', lw=0.5)
    ax11.annotate(f"{row['segment_name']}\n({row['count']:,})",
                  xy=(row['avg_freq'], row['avg_clv']),
                  xytext=(row['avg_freq'] + 0.1, row['avg_clv'] + 5),
                  color=WHITE, fontsize=8,
                  arrowprops=dict(arrowstyle='->', color=GRAY, lw=0.7))
ax11.set_xlabel('Avg Purchase Frequency', color=GRAY)
ax11.set_ylabel('Avg 12-Month CLV ($)', color=GRAY)
ax11.set_title('Customer Segments\n(Bubble size = segment size)',
               color=WHITE, fontsize=12, fontweight='bold')

plt.tight_layout()
plt.savefig('/content/viz_10_11_kmeans.png', dpi=150,
            bbox_inches='tight', facecolor=DARK_BG)
plt.show()
print(' VIZ 10, 11 saved')