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
│                          STREAMING STAGE                             │
│                                                                      │
│  producer.py  ──> Topic 'orders' ──> subscriber.py ──> PostgreSQL   │
│  (Generate)      (JSONL Stream)    (Flag Fraud)      (Orders Table) │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           BATCH STAGE                                │
│                                                                      │
│  ┌──────────────┐   DAG 1 (Hourly)   ┌──────────────┐             │
│  │   Airflow    │ ──────────────────> │ PostgreSQL   │             │
│  │  (Generate)  │                     │ Users/Products│             │
│  └──────────────┘                     └──────────────┘             │
│                                             │                        │
│                                             │ DAG 2 (Daily - H-1)   │
│                                             ▼                        │
│                                      ┌──────────────┐               │
│                                      │  BigQuery    │               │
│                                      │ (Partitioned)│               │
│                                      └──────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          DBT TRANSFORMATION                          │
│                                                                      │
│  Bronze (Staging) ──> Silver (Dim/Fact) ──> Gold (Data Marts)      │
│  • Deduplication    • Dimension Tables    • 5 Analytics Marts       │
│  • Data Cleaning    • Fact Tables         • Aggregations            │
│  • Views            • Tables              • Business Metrics        │
└─────────────────────────────────────────────────────────────────────┘
```

---
