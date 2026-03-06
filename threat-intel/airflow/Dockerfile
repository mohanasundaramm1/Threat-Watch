FROM apache/airflow:2.9.3

# Install libgomp (OpenMP) required by LightGBM's native C library
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && \
    rm -rf /var/lib/apt/lists/*
USER airflow

# Install Python dependencies at build time (faster startup than _PIP_ADDITIONAL_REQUIREMENTS)
RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    pyarrow \
    requests \
    dnspython \
    tldextract \
    "great_expectations==0.18.12" \
    scikit-learn \
    lightgbm \
    joblib
