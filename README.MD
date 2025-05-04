## 📊 YouTube API – Data Warehouse & Analytics Solution

This repository demonstrates a complete data pipeline that extracts data from the **YouTube Data API**, models it using the **Medallion Architecture**, and delivers business-ready insights via **Grafana dashboards**.

---

## 📦 Project Summary

This project implements a modern analytics pipeline with:

* **Medallion Architecture**: Structured into **Bronze**, **Silver**, and **Gold** layers for scalable data processing.
* **ETL Workflows**: Automated extraction, transformation, and loading using **Apache Airflow**.
* **Data Modeling**: Dimensional modeling in PostgreSQL for optimized querying.
* **Dashboards**: Real-time reporting using **Grafana**, powered by SQL.

---

## 🧰 Tech Stack

* **PostgreSQL** – Central data warehouse
* **Apache Airflow** – Workflow orchestration
* **Grafana** – Real-time data visualization
* **Linux VM** – Compute environment for pipeline execution
* **Python** – API ingestion & transformation logic

---

## 🎯 Project Objectives

Build a production-ready analytics solution to analyze YouTube channel and video performance:

* Source structured data from the YouTube Data API
* Clean, validate, and model for business intelligence
* Persist historical metrics (views, likes, etc.) for trend analysis
* Deliver actionable insights via dashboards and SQL queries

---

## 🗃️ Data Architecture (Medallion Model)

This project follows a **Bronze → Silver → Gold** pipeline:

![Architecture](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/s7h14w1q0s19j7kd4bxr.png)

### 🔹 Bronze Layer

Raw ingestion from the YouTube API (JSON format)

### 🔸 Silver Layer

Cleaned, validated, and structured data (see data flow and model below)

* **Data Flow**
  ![DataFlow](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/m5zcwddaic2eloxrbfgi.png)

* **Data Model**
  ![Data Model](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/4cus0akmrdna6t7y7yqf.png)

### 🟡 Gold Layer

Aggregated data used to generate KPIs and dashboards in Grafana

* **Visualization Sample**
  ![Visualization](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/2e2goyx5dw6yvs6548p3.png)

---

## 📈 BI Use Cases

Dashboards and SQL queries answer key questions such as:

* What are the **top-performing videos** per channel?
* How is each **channel performing over time**?
* What are the **daily trends** for views and engagement?

---

## 📁 Repository Structure

```text
├── README.md
├── channel_lists.py
├── channel_overview.py
├── channel_videos.py
├── __pycache__/                        # Compiled Python files
├── project_files/
│   ├── Architecture/                  # Draw.io and PNG files for architecture
│   └── ddl_update_scripts/           # SQL DDLs and procedures
│       ├── dim_channels.sql
│       ├── dim_videos.sql
│       ├── fct_subscribers_views_video_count.sql
│       └── fct_video_statistics.sql
└── requirements.txt                   # Python dependencies
```
