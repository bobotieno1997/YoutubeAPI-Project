## 📊 YouTube API – Data Warehouse and Analytics Project

Welcome to the **YouTube API – Data Warehouse and Analytics Project** repository! 🚀
This project demonstrates how to source data from the YouTube API and model it effectively to serve analytical needs.

---

## 📖 Project Overview

This project focuses on building a modern data analytics pipeline, including:

1. **Data Architecture**: Implementing the Medallion Architecture with **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from YouTube into a structured warehouse.
3. **Data Modeling**: Creating fact and dimension tables optimized for analysis.
4. **Analytics & Reporting**: Using SQL and Grafana dashboards to generate actionable insights.

---

## 🛠️ Tools & Technologies

All tools used are open-source and free:

* PostgreSQL (Database)
* Virtual Machine (Compute Environment)
* Apache Airflow (Workflow Orchestration)
* Grafana (Visualization & Dashboards)

---

## 🚧 Project Requirements

### 🎯 Objective

Develop a modern data warehouse using PostgreSQL to consolidate data from YouTube channels and videos, supporting data-driven decision-making.

### ✅ Key Specifications

* **Data Source**: YouTube Data API
* **Data Quality**: Implement cleaning and validation before analysis
* **Integration**: Merge data into a user-friendly, query-optimized model
* **Historical Data**: Store time-series data to track trends (e.g., daily views)

---

## 📈 Business Intelligence & Reporting

### 🎯 Objective

Generate SQL-based insights and visual dashboards, including:

* **Top-performing videos by channel**
* **Channel performance over time**

---

## 🎦 Data Architecture: Medallion Design

This project uses the **Medallion Architecture** to organize data across three layers:

![Data Architecture](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/s7h14w1q0s19j7kd4bxr.png)

1. **Bronze Layer**: Raw data ingested directly from the YouTube API.
2. **Silver Layer**: Cleaned and standardized data, structured for modeling.
3. **Gold Layer**: Aggregated views used for insights and visualizations via Grafana.

---
 ## 📂 This is the repository folder

````
├── README.MD
├── __pycache__
│   ├── channel_lists.cpython-312.pyc
│   ├── channel_overview.cpython-312.pyc
│   └── logging.cpython-312.pyc
├── channel_lists.py
├── channel_overview.py
├── channel_videos.py
├── project_files
│   ├── Architecture
│   │   ├── DataFlow.drawio
│   │   ├── DataFlow.png                  --- Dataflow Image
│   │   ├── DataModel.png                 --- DataModel in Silver Layer
│   │   ├── DataModel.png:Zone.Identifier
│   │   ├── OverviewArchitecture.drawio
│   │   └── OverviewArchitecture.png      --- Data Architecture
│   └── ddl_update_scripts
│       ├── dim_channels.sql    --DDL and Stored Procedure to update table
│       ├── dim_videos.sql      --DDL and Stored Procedure to update table
│       ├── fct_subscribers_views_video_count.sql --DDL and Stored Procedure to update table
|       |
│       └── fct_video_statistics.sql --DDL and Stored Procedure to update table
└── requirments.txt
```
