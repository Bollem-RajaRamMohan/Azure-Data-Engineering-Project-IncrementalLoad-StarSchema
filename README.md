# 🚀 End-to-End Data Engineering Pipeline (Medallion Architecture)

This project demonstrates a **modern data platform implementation** using **Azure Data Factory, Azure Data Lake Gen2, Databricks, and Delta Lake**, designed around the **Medallion Architecture (Bronze → Silver → Gold)**. It covers **data ingestion, transformation, and serving**, enabling **incremental data processing, scalable transformations, and analytics-ready models**.

---

## 🏗️ Architecture

![Project Workflow](Project_workflow.png)

### Flow Overview:
1. **Source Systems**: Data originates from SQL databases (and can be extended to other systems).  
2. **Ingestion (ADF)**: Azure Data Factory ingests data into **Data Lake Gen2** in **Parquet format**.  
3. **Bronze Layer (Raw Data Store)**:  
   - Stores raw, incremental data for traceability.  
   - Serves as the single source of truth.  
4. **Silver Layer (Transformed Data)**:  
   - Data is cleansed, standardized, and enriched using **Databricks**.  
   - Consolidates into a **One Big Table** for easier access.  
5. **Gold Layer (Serving Layer)**:  
   - Optimized **Delta Lake tables** prepared for analytics.  
   - Modeled into **Star Schema** for BI tools, dashboards, and machine learning.  
6. **Version Control (GitHub)**: All pipeline definitions, configurations, and notebooks are tracked in GitHub for collaboration and CI/CD.  

---

## ✨ Features

- ✅ **Incremental Ingestion** using Azure Data Factory  
- ✅ **Parquet-based storage** on Azure Data Lake Gen2  
- ✅ **Medallion Architecture (Bronze, Silver, Gold)**  
- ✅ **Transformations in Databricks** with Delta Lake  
- ✅ **Consolidated Silver Layer ("One Big Table")**  
- ✅ **Curated Gold Layer** using **Star Schema** for analytics  
- ✅ **Integration with GitHub** for version control and CI/CD  

---

## ⚙️ Tech Stack

- **Azure Data Factory (ADF)** → Orchestration & ingestion  
- **Azure Data Lake Gen2** → Centralized data storage (Parquet format)  
- **Databricks** → Transformations, Medallion architecture implementation  
- **Delta Lake** → Optimized storage format with ACID transactions  
- **GitHub** → Source control for pipelines, notebooks, and configs  

