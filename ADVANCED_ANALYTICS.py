fig, axes = plt.subplots(1, 2, figsize=(18, 7), facecolor=DARK_BG)
fig.suptitle('📈  VIZ 20–21: Customer Purchase Journey & Basket Analysis (BigQuery UNNEST)',
             fontsize=14, fontweight='bold', color=WHITE, y=1.02)

ax20 = axes[0]
ax20.set_facecolor(CARD_BG)
ax20.barh(
    df_basket.sort_values('users_in_basket')['category_item'],
    df_basket.sort_values('users_in_basket')['users_in_basket'],
    color=plt.cm.plasma(np.linspace(0.2, 0.9, len(df_basket)))
)
ax20.set_title('Categories in Multi-Cat Baskets\n(BigQuery UNNEST arrays)',
               color=WHITE, fontsize=12, fontweight='bold')
ax20.set_xlabel('Users in Cross-Category Basket', color=GRAY)

ax21 = axes[1]
ax21.set_facecolor(CARD_BG)
funnel_data = {
    'Page View':    df_events[df_events.event_type=='page_view'].shape[0],
    'Search':       df_events[df_events.event_type=='search'].shape[0],
    'Add to Cart':  df_events[df_events.event_type=='add_to_cart'].shape[0],
    'Purchase':     df_events[df_events.event_type=='purchase'].shape[0],
    'Return':       df_events[df_events.event_type=='return'].shape[0],
}
cols_funnel = [ACCENT, YELLOW, ORANGE, GREEN, RED]
for i, (stage, count) in enumerate(funnel_data.items()):
    ax21.barh(stage, count, color=cols_funnel[i], height=0.6)
    conv = count / funnel_data['Page View'] * 100
    ax21.text(count + 50, i, f'{count:,} ({conv:.1f}%)',
              va='center', fontsize=10, color=WHITE)
ax21.set_title('Retail Purchase Funnel\n(Kafka event types)',
               color=WHITE, fontsize=12, fontweight='bold')
ax21.set_xlabel('Event Count', color=GRAY)
ax21.invert_yaxis()

plt.tight_layout()
plt.savefig('/content/viz_20_21_advanced.png', dpi=150,
            bbox_inches='tight', facecolor=DARK_BG)
plt.show()
print(' VIZ 20, 21 saved')
print('\n All 21 visualizations complete!')