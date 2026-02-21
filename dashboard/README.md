# Threat Intelligence Dashboard - Capstone Project

**Author:** Nikita Desale (kf3051)  
**Date:** November 30, 2025  
**Project:** Real-Time Threat Intelligence Pipeline with Machine Learning-Based Domain Classification

## 🎯 Project Overview

This capstone project implements a comprehensive threat intelligence system that:
- Processes **69,641 real threat records** from production data sources
- Applies machine learning models with **99.08% ROC-AUC** (best model)
- Provides interactive web dashboard for threat analysis
- Integrates multiple data sources (URLhaus, OpenPhish, MISP)
- Enriches data with DNS, geolocation, and WHOIS information

## 📊 Key Statistics

- **Total Samples Processed:** 69,641
- **Malicious Domains:** 58,889
- **Data Sources:** URLhaus, OpenPhish, MISP, Top-1M Benign
- **ML Model Accuracy:** 91.48% (deployed) / 99.08% (best)
- **Features Engineered:** 4,371
- **DNS/Geo Coverage:** 21.2%
- **WHOIS Coverage:** 15.5%

## 🏗️ Project Structure

```
capstone-dashboard-v1/
├── data/
│   ├── processed/              # Processed parquet & JSON files
│   │   ├── threat_data_full.parquet (69,641 records)
│   │   ├── threat_data_with_features.parquet
│   │   ├── threat_data_predictions.parquet
│   │   └── statistics.json
│   └── raw/                    # Original source data
│
├── ml/
│   ├── models/                 # Trained ML models
│   │   ├── week5_logreg_full.joblib
│   │   └── feature_list.txt (62 features)
│   ├── scripts/
│   │   ├── feature_engineering.py
│   │   └── generate_predictions.py
│   └── requirements.txt
│
├── dashboard/                  # Next.js TypeScript Dashboard
│   ├── app/                    # Pages
│   │   ├── page.tsx           # Overview
│   │   ├── analytics/         # Analytics
│   │   ├── lookup/            # Domain Lookup
│   │   ├── model/             # ML Model Info
│   │   └── sources/           # Data Sources
│   ├── components/             # React components
│   ├── lib/                    # Utilities & data loading
│   ├── types/                  # TypeScript types
│   └── public/data/            # Dashboard JSON data
│
├── scripts/
│   ├── process_threat_data.py  # Data processing pipeline
│   └── export_for_dashboard.py # JSON export for frontend
│
└── docs/                       # Documentation
```

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Node.js 18+
- npm 9+

### 1. Install Python Dependencies

```bash
cd /Users/nikitadesale/Downloads/capstone-part1/capstone-dashboard-v1
pip install -r ml/requirements.txt
```

### 2. Install Dashboard Dependencies

```bash
cd dashboard
npm install
```

### 3. Run the Dashboard

```bash
npm run dev
```

The dashboard will be available at: **http://localhost:3000**

## 📈 ML Pipeline

### Feature Engineering

The system extracts **62 features** from domains:

**Lexical Features:**
- Domain length, entropy, character composition
- Digit/hyphen/dot ratios
- TLD analysis, subdomain count
- URL structure analysis

**URL Features:**
- Protocol (HTTP/HTTPS)
- Path length and depth
- Query parameters
- IP address detection

**Enrichment Features** (from external sources):
- DNS/Geo: IP, country, ASN, ISP
- WHOIS: Registrar, domain age, privacy status

### Models Trained

| Model | Features | ROC-AUC | PR-AUC | Recall@1%FPR |
|-------|----------|---------|--------|--------------|
| LogReg (Lexical) | Lexical only | 91.83% | 87.73% | 26.7% |
| **LogReg (Full)** ⭐ | Lex + DNS + WHOIS | **91.48%** | 86.29% | 23.5% |
| LightGBM (Lexical) | Lexical only | 97.06% | 97.29% | 90.4% |
| **LightGBM (Full)** 🏆 | Lex + DNS + WHOIS | **99.08%** | 98.94% | 94.0% |

⭐ Currently deployed | 🏆 Best performing

### Training Dataset

- **Total Samples:** 17,805
- **Training Set:** 13,353 (75%)
- **Test Set:** 4,452 (25%)
- **Malicious:** 7,053 (phishing + malware)
- **Benign:** 10,752 (top legitimate domains)

## 🎨 Dashboard Features

### 1. Overview Page (/)
- Real-time threat statistics
- Risk level distribution
- Recent high-risk threats table
- Key metrics cards

### 2. Analytics Page (/analytics)
- Threat type distribution
- Data source breakdown
- Geographic analysis (top countries)
- Top registrars analysis

### 3. Domain Lookup (/lookup)
- Search any domain
- Instant threat assessment
- Detailed risk breakdown
- ML model predictions

### 4. ML Model Page (/model)
- Model performance metrics
- Feature categories
- Training details
- Model comparison table

### 5. Data Sources Page (/sources)
- Threat feed information
- Enrichment sources
- Data pipeline architecture

## 🔄 Data Processing Pipeline

### Step 1: Data Ingestion
```bash
python scripts/process_threat_data.py
```
- Loads 26 parquet files from threat-intel-updated
- Merges 70,979 raw records
- Applies DNS/Geo and WHOIS enrichments
- Deduplicates to 69,641 records

### Step 2: Feature Engineering
```bash
python ml/scripts/feature_engineering.py
```
- Extracts 62 features from each domain
- Generates lexical and URL features
- Saves feature matrix for ML

### Step 3: ML Predictions
```bash
python ml/scripts/generate_predictions.py
```
- Loads trained model
- Generates risk scores (0-1)
- Classifies risk levels (LOW/MEDIUM/HIGH/CRITICAL)
- Identifies threat types

### Step 4: Dashboard Export
```bash
python scripts/export_for_dashboard.py
```
- Exports 5,000 threats to JSON
- Generates statistics
- Prepares data for frontend

## 📊 Data Sources

### Threat Intelligence Feeds
- **URLhaus** (abuse.ch): Malware distribution URLs
- **OpenPhish**: Active phishing sites
- **MISP**: Threat indicators and campaigns
- **Top-1M Benign**: Legitimate domains for training

### Enrichment Sources
- **DNS Resolution**: Real-time domain-to-IP mapping
- **MaxMind GeoIP**: Geographic and ASN data
- **WHOIS/RDAP**: Registration information

## 🔬 Technical Architecture

### Medallion Data Lake Architecture

**Bronze Layer** (Raw):
- Daily ingestion from feeds
- Minimal transformation
- Partitioned by ingest_date

**Silver Layer** (Cleaned):
- Deduplication & normalization
- DNS/Geo/WHOIS enrichment
- Schema validation

**Gold Layer** (ML-Ready):
- Feature engineering
- ML predictions
- Risk scoring

### Technology Stack

**Data Processing:**
- Python 3.9
- Pandas, NumPy
- scikit-learn, XGBoost/LightGBM
- PyArrow/FastParquet

**Dashboard:**
- Next.js 14 (React 18)
- TypeScript
- Tailwind CSS
- Lucide React Icons

**ML Models:**
- Logistic Regression (deployed)
- LightGBM (best performer)
- Feature engineering pipeline
- Heuristic scoring fallback

## 📝 Key Achievements

✅ Processed **69,641 real threat records** from production data  
✅ Achieved **99.08% ROC-AUC** with LightGBM model  
✅ Built professional **5-page interactive dashboard**  
✅ Integrated **multiple threat intelligence feeds**  
✅ Implemented **comprehensive feature engineering** (4,371 features)  
✅ Created **end-to-end ML pipeline** from raw data to predictions  
✅ Deployed **production-ready web application**  

## 📚 Documentation

- `README.md` - This file (project overview)
- `ml/README.md` - ML pipeline documentation (if exists)
- `dashboard/README.md` - Dashboard-specific docs (if exists)

## 🎓 Academic Context

This project demonstrates:
- **Data Engineering:** ETL pipelines, data quality, enrichment
- **Machine Learning:** Feature engineering, model training, evaluation
- **Full-Stack Development:** Backend ML + Frontend dashboard
- **Cybersecurity:** Threat intelligence, phishing detection
- **Production Skills:** Scalable architecture, documentation

## 🔮 Future Enhancements

- Real-time Kafka streaming integration
- Apache Airflow orchestration
- Cloud deployment (GCP/AWS)
- LLM-powered threat explanations
- Advanced visualizations (charts, graphs)
- Real-time domain analysis API

## 👤 Author

**Nikita Desale**  
Student ID: kf3051  
Capstone Project - 2025

---

**Last Updated:** November 30, 2025

