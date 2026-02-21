# Week 5 – Offline phishing baseline

## Run metadata

- Date run: 2025-11-29
- Scripts:
  - ml/offline/train_week5_baselines.py
  - ml/offline/analyze_week5_risk_clusters.py
  - ml/offline/check_enrichment_coverage.py
- Models saved:
  - ml/models/week5_logreg_lex.joblib
  - ml/models/week5_logreg_full.joblib
  - (optional) LightGBM text dumps: ml/models/week5_lgbm_*.txt
- Feature snapshot:
  - ml/tmp/week5_Xdf.parquet

## Data ranges

- Phishing days:
  - 2025-11-17 … 2025-11-26
- Benign days:
  - 2025-10-28, 2025-10-29, 2025-10-30, 2025-10-31

## Coverage (check_enrichment_coverage.py)

- Total benign registered domains: 10752
- DNS-Geo coverage: ~85.1% of benign domains
- WHOIS coverage: ~100% of benign domains

## Baseline metrics

### Logistic Regression

- **LogReg[lex_only]**
  - ROC-AUC: 0.9183
  - PR-AUC: 0.8773
  - Recall@FPR=1%: 0.267

- **LogReg[full]** (lex + DNS + WHOIS)
  - ROC-AUC: 0.9387
  - PR-AUC: 0.8921
  - Recall@FPR=1%: 0.260

### LightGBM

- **LightGBM[lex_only]**
  - ROC-AUC: 0.9706
  - PR-AUC: 0.9729
  - Recall@FPR=1%: 0.904

- **LightGBM[full]**
  - ROC-AUC: 0.9908
  - PR-AUC: 0.9900
  - Recall@FPR=1%: 0.942

## Notes

- Temporal split by ingest_date was degenerate; we fell back to a random stratified split:
  - Train size: 13353 (5289 positive, 8064 negative)
  - Test size: 4452 (1764 positive, 2688 negative)
- DNS coverage for benign is imperfect (no dns_geo on 2025-10-28..30), but missing DNS is handled by zero-filling.
- WHOIS coverage for benign is complete thanks to the final 2025-10-31 WHOIS run completing.
