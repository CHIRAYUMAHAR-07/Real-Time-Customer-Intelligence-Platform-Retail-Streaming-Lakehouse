fig, axes = plt.subplots(1, 2, figsize=(18, 6), facecolor=DARK_BG)
fig.suptitle('  VIZ 16–17: Flink Streaming Windows & Throughput',
             fontsize=16, fontweight='bold', color=WHITE, y=1.02)

ax16 = axes[0]
ax16.set_facecolor(CARD_BG)
win_ts = df_windows.groupby('window_ts')['revenue'].sum().reset_index()
win_ts = win_ts.sort_values('window_ts').head(200)
ax16.plot(win_ts['window_ts'], win_ts['revenue'],
          color=ACCENT, lw=1.5, alpha=0.85)
ax16.fill_between(win_ts['window_ts'], win_ts['revenue'], alpha=0.15, color=ACCENT)
ax16.set_title('Revenue per 1-Min Tumbling Window\n(Flink real-time aggregation)', color=WHITE,
               fontsize=12, fontweight='bold')
ax16.set_xlabel('Time', color=GRAY)
ax16.set_ylabel('Revenue ($)', color=GRAY)
ax16.tick_params(axis='x', rotation=30)

ax17 = axes[1]
ax17.set_facecolor(CARD_BG)
pctls     = [10, 25, 50, 75, 90, 95, 99]
pctl_vals = [np.percentile(df_events['flink_latency_ms'], p) for p in pctls]
ax17.plot(pctls, pctl_vals, 'o-', color=ACCENT, lw=2.5, ms=8)
ax17.fill_between(pctls, pctl_vals, alpha=0.12, color=ACCENT)
ax17.axhline(8000, color=RED, lw=2, ls='--', label='SLA: 8000ms')
for p, v in zip(pctls, pctl_vals):
    ax17.annotate(f'{v:.0f}ms', xy=(p, v), xytext=(p, v + 150),
                  ha='center', fontsize=9, color=WHITE)
ax17.set_xlabel('Percentile', color=GRAY)
ax17.set_ylabel('Latency (ms)', color=GRAY)
ax17.set_title('Flink End-to-End Latency\nPercentile Distribution', color=WHITE,
               fontsize=12, fontweight='bold')
ax17.legend(facecolor=CARD_BG, labelcolor=WHITE)

plt.tight_layout()
plt.savefig('/content/viz_16_17_flink.png', dpi=150,
            bbox_inches='tight', facecolor=DARK_BG)
plt.show()
print(' VIZ 16, 17 saved')