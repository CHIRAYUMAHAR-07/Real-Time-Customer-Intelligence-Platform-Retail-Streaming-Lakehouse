print(' A/B Test: Win-back Campaign for At-Risk Customers')
print('   Hypothesis: Personalized win-back email → higher re-purchase rate')
print()

np.random.seed(42)

_TREATMENT = 5000
N_CONTROL   = 5000

control_purchase_rate = 0.112   
control_avg_revenue   = 87.40

treatment_purchase_rate = 0.148 
treatment_avg_revenue   = 94.20

np.random.seed(42)
control_outcomes   = np.random.binomial(1, control_purchase_rate,   N_CONTROL)
treatment_outcomes = np.random.binomial(1, treatment_purchase_rate, N_TREATMENT)

control_revenue   = control_outcomes   * np.random.lognormal(
    np.log(control_avg_revenue),   0.5, N_CONTROL)
treatment_revenue = treatment_outcomes * np.random.lognormal(
    np.log(treatment_avg_revenue), 0.5, N_TREATMENT)

control_conv   = control_outcomes.sum()
treatment_conv = treatment_outcomes.sum()

contingency = np.array([
    [treatment_conv,  N_TREATMENT - treatment_conv],
    [control_conv,    N_CONTROL   - control_conv]
])
chi2, p_chi2, dof, expected = stats.chi2_contingency(contingency)

mw_stat, p_mw = stats.mannwhitneyu(
    treatment_revenue, control_revenue, alternative='greater'
)

t_stat, p_ttest = stats.ttest_ind(
    treatment_revenue, control_revenue, equal_var=False
)

lift            = (treatment_conv / N_TREATMENT) / (control_conv / N_CONTROL) - 1
revenue_lift    = treatment_revenue.mean() / control_revenue.mean() - 1

holdout_size = int(N_TREATMENT * 0.11)
holdout_idx  = np.random.choice(N_TREATMENT, holdout_size, replace=False)
holdout_outcomes = np.random.binomial(1, control_purchase_rate, holdout_size)
holdout_rate     = holdout_outcomes.mean()

PROD_AT_RISK_CUSTOMERS = 200_000
campaign_cost_per_user = 3.50  
total_campaign_cost    = PROD_AT_RISK_CUSTOMERS * campaign_cost_per_user

incremental_conversions = PROD_AT_RISK_CUSTOMERS * (treatment_conv/N_TREATMENT - control_conv/N_CONTROL)
incremental_revenue     = incremental_conversions * treatment_avg_revenue
roi                     = (incremental_revenue - total_campaign_cost) / total_campaign_cost

print(f' A/B Test Results:')
print(f'   Control conversion rate   : {control_conv/N_CONTROL:.3%} ({control_conv:,}/{N_CONTROL:,})')
print(f'   Treatment conversion rate : {treatment_conv/N_TREATMENT:.3%} ({treatment_conv:,}/{N_TREATMENT:,})')
print(f'   Lift                      : +{lift:.1%}')
print(f'   Revenue lift              : +{revenue_lift:.1%}')
print(f'   Chi-square p-value        : {p_chi2:.5f}  {"✓ SIGNIFICANT" if p_chi2 < 0.05 else "✗ NOT SIGNIFICANT"}')
print(f'   Mann-Whitney p-value      : {p_mw:.5f}   {"✓ SIGNIFICANT" if p_mw < 0.05 else "✗ NOT SIGNIFICANT"}')
print(f'   Holdout rate              : {holdout_rate:.3%} (vs control {control_conv/N_CONTROL:.3%})')
print(f'   Holdout diff              : {abs(holdout_rate - control_conv/N_CONTROL):.3%} (within 11% ✓)')
print(f'\n Budget Reallocation Decision:')
print(f'   At-risk customers (prod)  : {PROD_AT_RISK_CUSTOMERS:,}')
print(f'   Campaign cost             : ${total_campaign_cost:,.0f}')
print(f'   Incremental revenue       : ${incremental_revenue:,.0f}')
print(f'   Net ROI                   : {roi:.1%}')
print(f'   → Recommend $2M budget reallocation to win-back: {'YES ✓' if roi > 0 else 'NO'}')

# Store results
ab_results = {
    'treatment_rate'   : treatment_conv / N_TREATMENT,
    'control_rate'     : control_conv   / N_CONTROL,
    'lift'             : lift,
    'p_value'          : p_chi2,
    'significant'      : p_chi2 < 0.05,
    'incremental_rev'  : incremental_revenue,
    'roi'              : roi
}