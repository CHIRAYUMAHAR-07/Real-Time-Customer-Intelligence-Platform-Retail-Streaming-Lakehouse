fig, axes = plt.subplots(1, 2, figsize=(16, 6), facecolor=DARK_BG)
fig.suptitle('🧪  VIZ 12–13: A/B Test — Win-back Campaign Results',
             fontsize=15, fontweight='bold', color=WHITE, y=1.02)

ax12 = axes[0]
ax12.set_facecolor(CARD_BG)

groups       = ['Control\n(No Campaign)', 'Treatment\n(Win-back Email)']
rates        = [control_conv/N_CONTROL, treatment_conv/N_TREATMENT]
cis          = [1.96*np.sqrt(r*(1-r)/N_CONTROL) for r in rates]
bar_colors12 = [GRAY, GREEN]

bars12 = ax12.bar(groups, [r*100 for r in rates],
                  color=bar_colors12, width=0.5, edgecolor=DARK_BG)
ax12.errorbar(range(2), [r*100 for r in rates],
              yerr=[c*100 for c in cis],
              fmt='none', color=WHITE, capsize=8, lw=2)

for bar, rate in zip(bars12, rates):
    ax12.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
              f'{rate:.2%}', ha='center', fontsize=13, fontweight='bold', color=WHITE)

ax12.set_ylabel('Conversion Rate (%)', color=GRAY)
ax12.set_title(f'Re-purchase Rate\np={p_chi2:.5f} {"★ Significant" if p_chi2<0.05 else ""}',
               color=WHITE, fontsize=12, fontweight='bold')
ax12.set_ylim(0, max([r*100 for r in rates]) * 1.3)

ax12.annotate(f'+{lift:.1%} lift',
              xy=(1, rates[1]*100), xytext=(0.5, rates[1]*100 + 0.8),
              fontsize=13, color=GREEN, fontweight='bold',
              arrowprops=dict(arrowstyle='->', color=GREEN))

ax13 = axes[1]
ax13.set_facecolor(CARD_BG)

ctrl_rev_nonzero = control_revenue[control_revenue > 0]
trmt_rev_nonzero = treatment_revenue[treatment_revenue > 0]
cap = np.percentile(np.concatenate([ctrl_rev_nonzero, trmt_rev_nonzero]), 99)

ax13.hist(ctrl_rev_nonzero[ctrl_rev_nonzero < cap],  bins=40,
          alpha=0.55, color=GRAY,  label=f'Control (n={len(ctrl_rev_nonzero):,})',
          density=True)
ax13.hist(trmt_rev_nonzero[trmt_rev_nonzero < cap],  bins=40,
          alpha=0.55, color=GREEN, label=f'Treatment (n={len(trmt_rev_nonzero):,})',
          density=True)
ax13.axvline(ctrl_rev_nonzero.mean(),  color=GRAY,  lw=2, ls='--',
             label=f'Ctrl mean: ${ctrl_rev_nonzero.mean():.0f}')
ax13.axvline(trmt_rev_nonzero.mean(),  color=GREEN, lw=2, ls='--',
             label=f'Trmt mean: ${trmt_rev_nonzero.mean():.0f}')
ax13.set_xlabel('Revenue per Converter ($)', color=GRAY)
ax13.set_title(f'Revenue Distribution (converters)\nMann-Whitney p={p_mw:.5f}',
               color=WHITE, fontsize=12, fontweight='bold')
ax13.legend(facecolor=CARD_BG, labelcolor=WHITE, fontsize=9)

plt.tight_layout()
plt.savefig('/content/viz_12_13_ab_test.png', dpi=150,
            bbox_inches='tight', facecolor=DARK_BG)
plt.show()
print(' VIZ 12, 13 saved')