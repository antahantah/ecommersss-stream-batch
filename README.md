# Final Project: E-Commerce Data Pipeline (Streaming Fraud Detection + Batch Analytics)

**Author:** Aditya Putra Ferza  
**Program:** Purwadhika Data Engineering Bootcamp  
**Date:** November - December 2025  

---

## Project Overview

A comprehensive **hybrid data pipeline** combining real-time fraud detection with batch analytics for an e-commerce marketplace platform (eCommer$$$).

### Real Life Simulation Business Problem

The eCommer$$$ online marketplace platform was experiencing significant losses due to fake or fraud transactions. Those effects are:
- Payment refund complications
- Poor seller ratings
- Revenue loss from fake transactions

### Troubleshooting Approach

Built an end-to-end data engineering pipeline with three main stages:
1. **Real-Time Fraud Detection** - Streaming pipeline flags suspicious orders instantly
2. **Batch Data Processing** - Automated ETL from PostgreSQL to BigQuery
3. **Analytics Layer** - dbt-powered data marts for fraud pattern analysis

---

## Technology Stack

| Category | Technology | Purpose |
|----------|------------|---------|
| **Containerization** | Docker, Docker Compose | Isolated environments for all services |
| **Orchestration** | Apache Airflow 2.7+ | Workflow automation and scheduling |
| **Databases** | PostgreSQL 12 | Operational data storage (Orders, Users, Products) |
| **Data Warehouse** | Google BigQuery | Analytics-ready data storage with partitioning |
| **Transformation** | dbt (Data Build Tool) | ELT transformations and data mart creation |
| **Streaming** | JSONL, Producer-Subscriber Pattern | Real-time order processing and fraud detection |
| **Language** | Python 3.7+ | Core programming and data processing |
| **Environment** | Virtual Environment (venv) | Management for streaming and dbt components |

---

## Architecture

### Complete Data Pipeline Flow
```
┌─────────────────────────────────────────────────────────────────────┐
│                          STREAMING STAGE                            │
│                                                                     │
│  producer.py  ──> Topic 'orders' ──> subscriber.py ──> PostgreSQL   │
│  (Generate)      (JSONL Stream)    (Flag Fraud)      (Orders Table) │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           BATCH STAGE                               │
│                                                                     │
│  ┌──────────────┐   DAG 1 (Hourly)   ┌──────────────┐               │
│  │   Airflow    │ ──────────────────> │ PostgreSQL   │              │
│  │  (Generate)  │                     │ Users/Products│             │
│  └──────────────┘                     └──────────────┘              │
│                                             │                       │
│                                             │ DAG 2 (Daily - H-1)   │
│                                             ▼                       │
│                                      ┌──────────────┐               │
│                                      │  BigQuery    │               │
│                                      │ (Partitioned)│               │
│                                      └──────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          DBT TRANSFORMATION                         │
│                                                                     │
│  Bronze (Staging) ──> Silver (Dim/Fact) ──> Gold (Data Marts)       │
│  • Deduplication    • Dimension Tables    • 5 Analytics Marts       │
│  • Data Cleaning    • Fact Tables         • Aggregations            │
│  • Views            • Tables              • Business Metrics        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Stages

### Stage 0: Database Setup

**Setup 3 PostgreSQL databases in Docker:**
- `orders` - Transaction data with fraud flags
- `users` - Customer information  
- `products` - Product catalog

**Configuration:**
- Image: `postgres:12` (modern yet compatible for old environment)
- Port: `5433:5432` (avoid conflicts with existing projects)
- Initialization: `db.py` script creates all tables

---

### Stage 1: Streaming Processing (Real-Time Fraud Detection)

**Components:**

1. **producer.py** - Order Generation
   - Generates new orders
   - Publishes to topic `'orders'` as JSONL stream
   - Simulates real-time transaction flow

2. **subscriber.py** - Fraud Detection & Loading
   - Subscribes to topic `'orders'`
   - Analyzes each order, line by line, for checking fraud patterns
   - Flags transactions as `fraud` or `genuine` by creating `status` for each line/object
   - Loads entire data (+ status) to PostgreSQL `orders` table

**Result:** Real-time fraud detection with instant flagging

---

### Stage 2: Batch Processing (Airflow Orchestration)

**Infrastructure:**
- Dockerized Airflow environment
- Isolated containers for all batch processes
- BigQuery integration outside Docker (public cloud)

#### Stage 2.1: Data Generation (DAG 1)

**Schedule:** Hourly (`0 * * * *`)

**Tasks:**
- Generate random **user** data with 3 email format options (using Airflow branch operator)
- Generate random **product** data
- Insert directly to PostgreSQL `users` and `products` tables

**Features:**
- Dynamic branching for email format selection
- Randomized data for realistic testing
- Automated hourly execution

#### Stage 2.2: PostgreSQL to BigQuery (DAG 2)

**Schedule:** Daily (processes yesterday data)

**ETL Flow:**
1. Extract yesterday's data from PostgreSQL
2. Stage data in Airflow `/tmp` folder as JSON files
3. Load to BigQuery with:
   - Pre-configured schema definitions
   - Date-based table partitioning
   - Incremental append mode
   - Google Cloud authentication

**Result:** Automated daily data warehouse updates with optimized partitioning

---

### Stage 3: Data Transformation (dbt - Data Build Tool)

**3-Layer Architecture:**

#### 🥉 Bronze Layer (Staging)
- **Type:** Views (auto-updated from BigQuery tables)
- **Purpose:** Raw data cleaning and preparation
- **Operations:**
  - Deduplication of records
  - Data type formatting and corrections
  - Update some columns name for standardization
- **Tables:** `stg_orders`, `stg_users`, `stg_products`

#### 🥈 Silver Layer (Dimension & Fact Tables)
- **Type:** Materialized Tables
- **Purpose:** Business logic and relationships
- **Operations:**
  - Select relevant columns
  - Create calculated fields
  - Create boolean for fraud status
  - Build dimension and fact tables
  - Establish data relationships
- **Tables:** `dim_users`, `dim_products`, `fact_orders`

#### 🥇 Gold Layer (Data Marts)
- **Type:** Materialized Tables
- **Purpose:** Business-ready analytics
- **Operations:**
  - Data aggregations by various dimensions
  - JOIN operations across tables
  - Business metric calculations
  - Multiple marts for different use cases

**5 Data Marts Created:**
1. **Users and Blacklist** - Users and their blacklist status from historical fraud transaction
2. **Most Fraudulent Users** - Users who occasionally doing fraud
3. **Payment Doing Fraud Critical Time** - Payment which doing fraud transaction at 12 - 4 AM
4. **Product and Users** - Users who occasionally buy same products (habits)
5. **Product and Payment Methods** - Most used payment method for purchasing a product

---

## Database Schema

### Orders Table
```sql
id              INTEGER PRIMARY KEY
user_id         INTEGER FOREIGN KEY → users.id
product_id      INTEGER FOREIGN KEY → products.id
amount          DECIMAL
payment_method  VARCHAR
is_fraud        BOOLEAN
created_date    TIMESTAMP
```

### Users Table
```sql
id              INTEGER PRIMARY KEY
username        VARCHAR
email           VARCHAR
created_date    TIMESTAMP
```

### Products Table
```sql
id              INTEGER PRIMARY KEY
product_name    VARCHAR
category        VARCHAR
price           DECIMAL
created_date    TIMESTAMP
```

---

## Key Results

### Performance Metrics
✅ **Real-time Detection:** Fraud flagging within seconds of order creation  
✅ **Automation:** Reduced manual reporting to fully automated workflows  
✅ **Data Quality:** 3-layer dbt transformation ensures clean, validated data  
✅ **Query Optimization:** Date-partitioned BigQuery tables for fast analytics  
✅ **Scalability:** Containerized architecture supports horizontal scaling  

### Business Impact
✅ **Instant fraud detection** reduces financial losses  
✅ **Automated analytics** enables data-driven decision making  
✅ **5 comprehensive data marts** support various business analyses  
✅ **Nightly batch processing** ensures fresh analytics data  

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.7+ with venv
- Google Cloud Platform account with BigQuery enabled
- GCP service account with BigQuery permissions

### Setup Instructions

1. **Clone Repository**
```bash
git clone https://github.com/antahantah/ecommersss-stream-batch.git
cd ecommersss-stream-batch
```

2. **Set Up Virtual Environment (for streaming components)**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. **Configure Environment Variables**
```bash
cp .env.example .env
# Edit .env with your configuration
```

4. **Add GCP Credentials**
```bash
# Place your service account JSON key
cp /path/to/your-key.json gcp-key.json
```

5. **Start Docker Services**
```bash
docker-compose up -d
```

6. **Initialize Databases - Must be on Virtual Environment**
```bash
python db.py  # Creates PostgreSQL tables
```

7. **Start Streaming Pipeline - Must be on Virtual Environment**
```bash
# Terminal 1: Start producer
python producer.py

# Terminal 2: Start subscriber
python subscriber.py
```

8. **Access Airflow**
- URL: http://localhost:8080
- Enable and trigger DAGs for batch processing

9. **Set Up Other Virtual Environment (for dbt)**
```bash
cd dbt_transformation
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt (don't be mistaken with first venv)
```

10. **Run dbt Transformations**
```bash
cd ./dbt_transformation/ecommersss/analytics
dbt run (or dbt run -s <folder path> for stg/dim/mart)
```

---

## 📂 Project Structure
```
ecommersss-stream-batch/
├── docker-compose.yaml           # Docker services configuration
├── dockerfile.airflow            # Custom Docker images for Airflow
├── .env                          # Environment variables
├── .gitignore                    # Git ignore rules
│
├── src/                          # Real-time fraud detection (Streaming)
│   ├── __init__.py               # Default file for Python Package Declaration 
│   ├── producer.py               # Order generation and publishing
│   ├── subscriber.py             # Fraud detection and loading
│   └── db.py                     # Database initialization
│
├── dags/                         # Dockerized Airflow for Batch Processing
│   ├── generate_users_postgres.py            # DAG 1: Generate users
│   ├── generate_products_postgres.py         # DAG 1: Generate products
│   ├── postgres_to_bq_users.py               # DAG 2: Load Table Users to BigQuery
│   ├── postgres_to_bq_products.py            # DAG 2: Load Table Products to BigQuery
│   ├── postgres_to_bq_orders.py              # DAG 2: Load Table Orders to BigQuery
│   └── gcp-key.json                          # Google Cloud credentials
│
├── dbt_project/                # dbt transformation logic
│   ├── models/
│   │   ├── staging/           # Bronze layer (views)
│   │   ├── core/              # Silver layer (dim/fact tables)
│   │   └── marts/             # Gold layer (analytics marts)
│   └── dbt_project.yml
│
└── README.md                   # This file
```

---

## Technical Highlights

### Streaming Architecture
- **Producer-Consumer Pattern** for real-time data processing
- **JSONL format** for efficient streaming data serialization
- **Immediate fraud flagging** without batch delays

### Batch Orchestration
- **Hourly data generation** with Airflow branching for dynamic workflows
- **Daily Yesterday (H-1) Data Load** ensures complete previous day data processing
- **JSON staging** in Airflow temp directory for reliable transfers

### Data Transformation
- **Bronze-Silver-Gold**
- **Data quality controls**
- **5 specialized data marts**

### Infrastructure
- **Multi-container Docker** setup for service isolation
- **Virtual environments** for streaming components
- **BigQuery partitioning** for performance optimization
- **GCP integration** with secure service account authentication

---

## Business Insights Enabled

### From Data Marts:

1. **Users and Blacklist**
   - Checking Users that doing fraud transaction
   - Giving warning flag or blacklist flag for users that occasionally doing fraud transactions

2. **Most Fraudulent Users**
   - Sorting by most fraud transactions by one users

3. **Payment Doing Fraud Critical Time**
   - Fraud correlation with payment types
   - Focusing transaction that happened in 12:00 - 04:59 AM

4. **Product and Users**
   - Grouping User and Product id from fact_orders
   - Gain insight about users habit from purchasing product repeatedly (habit buying)

5. **Product and Payment Methods**
   - Grouping Product id and Payment Method from fact_orders
   - Gain insight about most payment method used for purchasing a product

---

## Future Improvements

### Recommended Enhancements:

1. **Additional Data Sources**
   - Add delivery and shipping status tables
   - Good for transaction cancelation, track refund, and return patterns

2. **Enhanced Data Generation**
   - Integrate `Faker` library for real-life simulation approach
   - Products and Users would be more realistic

3. **Infrastructure**
   - Implement actual message queue (Kafka or Pub/Sub)
   - Add monitoring and alerting

5. **Data Marts**
   - Created Date utilization for product freshness 
   - Customer lifetime value analysis (loyalty, payment method habit, multiple fraud orders attempt)
   - Cohort analysis with fraud segmentation

---

## Screenshots

### Docker Environment
WIP

### Streaming Pipeline
WIP

### Airflow DAGs
WIP

### BigQuery Tables
WIP

### dbt Lineage
WIP


---

## Learning Outcomes

This project demonstrates proficiency in:

✅ **Real-time data processing** and streaming architectures  
✅ **Workflow orchestration** with Apache Airflow  
✅ **Data warehousing** with Google BigQuery  
✅ **ELT transformations** using dbt  
✅ **Containerization** for reproducible data environments  
✅ **Data modeling** with Three Primary Layer Approach (Staging, Intermediate, Mart) 
✅ **Production best practices** including partitioning, incremental loads, and data quality  

---

## 👤 Author

**Aditya Putra Ferza**
- **GitHub:** [@antahantah](https://github.com/antahantah)
- **LinkedIn:** [Aditya Putra Ferza](https://linkedin.com/in/aditya-putra-ferza)
- **Email:** adit.ferza@gmail.com
- **Cohort:** Purwadhika JCDEAH-006

*This project was created as the final project for Purwadhika's Data Engineering Bootcamp, demonstrating end-to-end data engineering skills in a realistic business scenario.*

---

## 🙏 Acknowledgements

- **Purwadhika Digital Technology School** for comprehensive curriculum and project guidance
- **The lecturers and Cohort peers** for collaboration, support, and guidance throughout the bootcamp
- **Google Cloud Platform** for BigQuery infrastructure
- **Open-source community** for Apache Airflow, dbt, and supporting tools

---

## 📄 License

This project is created for educational purposes as part of a data engineering bootcamp final project.

---

## 🔗 Related Projects

Check out my other data engineering project:
- **[Automated ETL Pipeline with Airflow](https://github.com/antahantah/simple-airflow-with-postgre)** - PostgreSQL to BigQuery batch processing with performance optimization

---

**⭐ If you found this project helpful or interesting, please star the repository!**

**💼 Open to Data Engineer opportunities! Let's connect on [LinkedIn](https://linkedin.com/in/aditya-putra-ferza).**

