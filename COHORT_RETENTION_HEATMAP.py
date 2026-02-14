fig, ax = plt.subplots(figsize=(16, 8), facecolor=DARK_BG)
fig.suptitle('🔷  VIZ 6: Cohort Retention Heatmap (BigQuery QUALIFY + Partitioned Windows)',
             fontsize=14, fontweight='bold', color=WHITE)

cohort_pivot = df_cohort.pivot_table(
    values='retention_pct',
    index='cohort_month',
    columns='months_since_cohort',
    aggfunc='mean'
)
cohort_pivot.index = [str(i)[:7] for i in cohort_pivot.index]

mask = cohort_pivot.isna()
sns.heatmap(
    cohort_pivot, ax=ax, mask=mask,
    annot=True, fmt='.1f', cmap='YlOrRd',
    vmin=0, vmax=100,
    linewidths=0.5, linecolor=DARK_BG,
    annot_kws={'size': 9, 'color': 'white'},
    cbar_kws={'label': 'Retention %', 'shrink': 0.8}
)
ax.set_xlabel('Months Since First Purchase', color=GRAY, fontsize=12)
ax.set_ylabel('Cohort Month', color=GRAY, fontsize=12)
ax.set_title('Monthly Cohort Retention (%) — 12-Month View', color=WHITE,
             fontsize=13, pad=15)
ax.tick_params(colors=GRAY)

plt.tight_layout()
plt.savefig('/content/viz_6_cohort_retention.png', dpi=150,
            bbox_inches='tight', facecolor=DARK_BG)
plt.show()
print(' VIZ 6 saved')