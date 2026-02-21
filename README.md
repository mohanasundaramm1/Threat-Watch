# 🎓 Capstone Project - Complete Package

**Team Members:**
- Mohan (vw4192) - Data Engineering
- Nikita (kf3051) - ML & Dashboard

---

## 📦 What's Included:

### 1. Backend (threat-intel/)
- ✅ Complete Airflow pipeline
- ✅ ML training scripts
- ✅ Data processing (Bronze/Silver layers)
- ✅ Enrichment (WHOIS, DNS, Geo)
- ✅ Real threat data

### 2. Frontend (dashboard/)
- ✅ Next.js TypeScript dashboard
- ✅ 6 pages (Home, Lookup, Analytics, Model, Sources, About)
- ✅ Real-time threat visualization
- ✅ Risk scoring

---

## 🚀 Quick Start:

### Backend Setup (5 mins):
```bash
cd threat-intel
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run ML training
python3 ml/offline/train_latest_baseline.py
```

### Dashboard Setup (5 mins):
```bash
cd dashboard
npm install
npm run dev
```

### Open Dashboard:
```
http://localhost:3000
```

---

## 📊 Data Included:

- **Threat Records:** 13,000+ URLs
- **Sources:** URLhaus + OpenPhish
- **Time Period:** Oct-Nov 2025
- **Enrichment:** WHOIS, DNS, Geolocation

---

## 🎯 Demo Features:

**Backend:**
- Airflow orchestration (10 DAGs)
- PySpark streaming
- ML models (99% accuracy)
- Data quality checks

**Dashboard:**
- Interactive threat lookup
- Risk scoring algorithm
- Analytics visualizations
- ML model metrics

---

## 📞 Questions?

Contact:
- Nikita: kf3051@kingstonuniversity.ac.uk
- Mohan: vw4192@kingstonuniversity.ac.uk

---

**Status:** ✅ Ready to Demo
**Last Updated:** February 2026
