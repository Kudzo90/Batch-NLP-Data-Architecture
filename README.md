# Babel NLP Pipeline

A batch-processing data architecture for NLP analysis of multilingual news articles, built as part of the DLMDSEDE02 Data Engineering portfolio project.

## 🏗️ Architecture Overview

This project implements a **Medallion Architecture** (Raw → Cleaned → Curated) with the following components:

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Orchestration** | Apache Airflow | Monthly batch scheduling, retries, backfills |
| **Compute** | Apache Spark | ETL, NLP enrichment, quarterly aggregations |
| **Storage** | MinIO (S3-compatible) | Data lake with raw, cleaned, curated zones |
| **Data Quality** | Great Expectations | Schema validation and data quality checks |
| **Analytical DB** | ClickHouse | Low-latency queries on curated aggregates |
| **Data Delivery** | FastAPI | REST API for downstream ML workflows |

### Data Flow

```
Raw JSON → Spark Ingest → Raw Partitioned (Parquet)
                ↓
         Spark Clean/Enrich (NLP, Sentiment)
                ↓
         Cleaned Zone (Parquet)
                ↓
         Spark Quarterly Aggregate
                ↓
         Curated Zone (Parquet) → ClickHouse → FastAPI → ML Consumers
```

## 📁 Project Structure

```
babel-nlp-pipeline/
├── airflow/
│   └── dags/
│       └── monthly_batch_dag.py      # Airflow DAG definition
├── spark/
│   └── jobs/
│       ├── ingest_all_languages.py   # Raw JSON → Partitioned Parquet
│       ├── clean_enrich.py           # NLP processing & sentiment analysis
│       └── quarterly_aggregate.py    # Quarterly aggregations
├── fastapi/
│   ├── main.py                       # REST API endpoints
│   ├── Dockerfile
│   └── requirements.txt
├── clickhouse_init/
│   └── init.sql                      # Auto-creates quarterly_stats table
├── great_expectations/
│   └── expectations/
│       └── news_suite.json           # Data quality expectations
├── data/                             # Data lake (excluded from git)
│   ├── raw/                          # Original JSON files
│   ├── raw_partitioned/              # Partitioned by year/month
│   ├── cleaned/                      # NLP-enriched data
│   └── curated/                      # Quarterly aggregates
├── docker-compose.yml                # Infrastructure as Code
├── generate_synthetic_data.py        # Creates test data
└── .gitignore
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- 8GB+ RAM recommended

### 1. Clone and Start

```bash
git clone https://github.com/Kudzo90/Batch-NLP-Data-Architecture.git
cd babel-nlp-pipeline

# Create data directories
mkdir -p data/raw data/raw_partitioned data/cleaned data/curated

# Start all services
docker-compose up -d
```

### 2. Verify Services

| Service | URL | Purpose |
|---------|-----|---------|
| Airflow | http://localhost:8080 | Orchestration UI |
| FastAPI | http://localhost:8000 | Data API |
| MinIO | http://localhost:9001 | Object storage UI |
| ClickHouse | localhost:8123 | Analytical database |

### 3. Generate Synthetic Data (Optional)

If you don't have the Babel Briefings dataset:

```bash
python generate_synthetic_data.py
```

### 4. Run the Pipeline

**Option A: Via Airflow UI**
1. Open http://localhost:8080
2. Enable the `monthly_nlp_batch` DAG
3. Trigger manually or wait for scheduled run

**Option B: Manual Spark Jobs**
```bash
# Ingest
docker exec -it spark-master /opt/spark/bin/spark-submit \
  /opt/spark-jobs/ingest_all_languages.py \
  --source /data/raw --target /data/raw_partitioned \
  --start 2020-07-01 --end 2020-08-31

# Clean & Enrich
docker exec -it spark-master /opt/spark/bin/spark-submit \
  /opt/spark-jobs/clean_enrich.py --month 2020-07

# Aggregate
docker exec -it spark-master /opt/spark/bin/spark-submit \
  /opt/spark-jobs/quarterly_aggregate.py --quarter 2020-07
```

### 5. Query the API

```bash
# List available datasets
curl http://localhost:8000/datasets

# Get Q3 2020 data
curl http://localhost:8000/quarterly/2020/3
```

## 📊 Dataset

This project uses the **Babel Briefings** dataset:
- 4.7 million multilingual news articles
- Coverage: 2017-2020 (varies by language file)
- Source: [Kaggle](https://www.kaggle.com/datasets/gpreda/babel-briefings)

The architecture is designed to scale to the full dataset, though demonstrations may use a subset due to resource constraints.

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ROOT_USER` | minioadmin | MinIO access key |
| `MINIO_ROOT_PASSWORD` | minioadmin | MinIO secret key |
| `AIRFLOW_UID` | 50000 | Airflow user ID |

### Airflow DAG Schedule

The `monthly_nlp_batch` DAG runs on the 1st of each month (`0 0 1 * *`), processing the previous month's data.

## 🧪 Data Quality

Great Expectations validates:
- Schema conformance (required columns present)
- Data types (timestamps, strings, numerics)
- Value ranges (sentiment scores between -1 and 1)
- Null checks on critical fields

## 📈 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check |
| `/health` | GET | Service status |
| `/datasets` | GET | List available curated datasets |
| `/quarterly/{year}/{quarter}` | GET | Fetch quarterly aggregated data |

### Example Response

```json
{
  "year": 2020,
  "quarter": 3,
  "count": 150,
  "data": [
    {
      "category": "politics",
      "article_count": 45,
      "avg_sentiment": 0.12
    }
  ]
}
```

## 🏛️ Design Decisions

### Why Medallion Architecture?
- Clear separation of concerns (raw → cleaned → curated)
- Enables reprocessing without re-ingesting
- Supports multiple downstream consumers

### Why Spark?
- Handles large-scale batch processing
- Native Parquet support
- Rich NLP libraries available

### Why ClickHouse?
- Columnar storage optimized for analytics
- Fast aggregation queries
- Scales horizontally

### Why Airflow?
- Industry-standard orchestration
- Built-in retry logic and backfills
- Visual DAG monitoring

## 🔮 Future Enhancements

- [ ] Implement streaming ingestion with Kafka
- [ ] Add more NLP features (NER, topic modelling)
- [ ] Deploy to Kubernetes for horizontal scaling
- [ ] Add Grafana dashboards for monitoring
- [ ] Implement CI/CD pipeline

## 📸 Demo Outputs

The `outputs/` folder contains:
- Airflow DAG execution screenshots (Grid/Graph views)
- Airflow main page
- FastAPI endpoint responses
- Data lake folder structure verification

## 📝 License

This project is submitted as part of the IU International University MSc Data Science program (DLMDSEDE02).

## 👤 Author

Wonder K. EKPE  
MSc Data Science  
IU International University
