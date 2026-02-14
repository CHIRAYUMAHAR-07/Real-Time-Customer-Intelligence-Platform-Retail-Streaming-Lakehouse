fig, axes = plt.subplots(1, 3, figsize=(20, 6), facecolor=DARK_BG)
fig.suptitle('📡  VIZ 1–3: Kafka Event Stream Analytics', fontsize=16,
             fontweight='bold', color=WHITE, y=1.02)

av1 = axes[0]
event_counts = df_events['event_type'].value_counts()
colors_bar = [ACCENT, GREEN, ORANGE, YELLOW, PURPLE, RED, '#FF69B4']
av1.bar(event_counts.index, event_counts.values,
        color=colors_bar[:len(event_counts)], edgecolor=DARK_BG, linewidth=0.5)
av1.set_title('Event Type Distribution\n(2M+ events/day)', color=WHITE, fontsize=12, fontweight='bold')
av1.set_ylabel('Event Count', color=GRAY)
av1.tick_params(axis='x', rotation=30, colors=GRAY)
for i, (_, v) in enumerate(event_counts.items()):
    av1.text(i, v + 50, f'{v:,}', ha='center', fontsize=8, color=WHITE)

av2 = axes[1]
latency_data = df_events['flink_latency_ms']
av2.hist(latency_data, bins=40, color=ACCENT, alpha=0.8, edgecolor=DARK_BG)
av2.axvline(latency_data.mean(),   color=YELLOW, lw=2, ls='--', label=f'Mean: {latency_data.mean():.0f}ms')
av2.axvline(latency_data.quantile(0.95), color=RED, lw=2, ls='--', label=f'P95: {latency_data.quantile(0.95):.0f}ms')
av2.axvline(8000, color=ORANGE, lw=2, ls='-', label='SLA: 8000ms')
av2.set_title('Flink End-to-End Latency\n(exactly-once semantics)', color=WHITE, fontsize=12, fontweight='bold')
av2.set_xlabel('Latency (ms)', color=GRAY)
av2.legend(fontsize=9, facecolor=CARD_BG, labelcolor=WHITE)

av3 = axes[2]
heatmap_data = df_events.pivot_table(
    values='event_id', index='event_dow', columns='event_hour', aggfunc='count', fill_value=0)
heatmap_data.index = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
sns.heatmap(heatmap_data, ax=av3, cmap='Blues',
            cbar_kws={'label': 'Events'}, linewidths=0.2)
av3.set_title('Event Volume Heatmap\n(Day × Hour)', color=WHITE, fontsize=12, fontweight='bold')
av3.set_xlabel('Hour of Day', color=GRAY)
av3.set_ylabel('Day of Week', color=GRAY)

plt.tight_layout()
plt.savefig('/content/viz_1_2_3_kafka_stream.png', dpi=150,
            bbox_inches='tight', facecolor=DARK_BG)
plt.show()
print(' VIZ 1, 2, 3 saved')