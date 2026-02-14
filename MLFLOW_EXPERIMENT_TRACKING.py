w.set_tracking_uri('/content/mlruns')
mlflow.set_experiment('retail_customer_intelligence')

with mlflow.start_run(run_name='bgnbd_clv_v2_champion'):
    mlflow.log_param('model_type',        'BG/NBD + GammaGamma')
    mlflow.log_param('penalizer_coef',    0.001)
    mlflow.log_param('observation_period_days', 547)
    mlflow.log_param('prediction_horizon_days', 365)
    mlflow.log_param('customers_modeled', len(rfm_model))

    mlflow.log_metric('avg_clv_12m',      rfm_model.clv_12m.mean())
    mlflow.log_metric('median_clv_12m',   rfm_model.clv_12m.median())
    mlflow.log_metric('top20_revenue_pct',top20_pct)
    mlflow.log_metric('avg_prob_alive',   rfm_model.prob_alive.mean())
    mlflow.log_metric('total_pred_revenue',total_clv)

    bgf_run = mlflow.active_run().info.run_id

with mlflow.start_run(run_name=f'kmeans_k{optimal_k}_rfm_clv'):
    mlflow.log_param('model_type',    'KMeans')
    mlflow.log_param('n_clusters',    optimal_k)
    mlflow.log_param('features',      str(FEATURES))
    mlflow.log_param('scaler',        'StandardScaler')

    mlflow.log_metric('silhouette_score', max(silhouettes))
    mlflow.log_metric('inertia',          kmeans_final.inertia_)
    mlflow.log_metric('n_customers',      len(df_seg))

    mlflow.sklearn.log_model(kmeans_final, 'kmeans_model')
    km_run = mlflow.active_run().info.run_id

with mlflow.start_run(run_name='ab_test_winback_campaign'):
    mlflow.log_param('test_type',       'Chi-square + Mann-Whitney')
    mlflow.log_param('n_treatment',     N_TREATMENT)
    mlflow.log_param('n_control',       N_CONTROL)
    mlflow.log_param('holdout_pct',     0.11)

    mlflow.log_metric('treatment_rate', ab_results['treatment_rate'])
    mlflow.log_metric('control_rate',   ab_results['control_rate'])
    mlflow.log_metric('lift',           ab_results['lift'])
    mlflow.log_metric('p_value',        ab_results['p_value'])
    mlflow.log_metric('incremental_rev',ab_results['incremental_rev'])
    mlflow.log_metric('roi',            ab_results['roi'])

    ab_run = mlflow.active_run().info.run_id

print(' MLflow Experiments Logged:')
print(f'   Experiment  : retail_customer_intelligence')
print(f'   Run 1 (CLV) : {bgf_run[:8]}... | Top-20% = {top20_pct:.1%} revenue')
print(f'   Run 2 (K-Means): {km_run[:8]}... | Silhouette = {max(silhouettes):.4f}')
print(f'   Run 3 (A/B) : {ab_run[:8]}... | p={ab_results["p_value"]:.5f} | ROI={ab_results["roi"]:.1%}')