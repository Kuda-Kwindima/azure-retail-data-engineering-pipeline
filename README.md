# Instacart Retail Analytics Warehouse Pipeline

This project builds an end-to-end data engineering pipeline that transforms the Instacart Online Grocery Shopping dataset into a structured PostgreSQL analytics warehouse.

The pipeline loads raw CSV data, cleans and models it, and produces analytics-ready data marts for business insights.

## Architecture

The warehouse follows a layered architecture:

Raw → Staging → Warehouse → Data Marts

Raw layer  
Stores the original source data exactly as received.

Staging layer  
Applies data cleaning and standardization.

Warehouse layer  
Builds dimensional models including fact and dimension tables.

Data marts  
Aggregated tables designed for analytics and reporting.

## Technologies

Python  
SQL  
PostgreSQL  
Prefect (pipeline orchestration)  
Pandas  
SQLAlchemy

## Pipeline Steps

1. Create raw tables
2. Load raw CSV data
3. Transform data into staging tables
4. Build dimensional warehouse tables
5. Generate analytics marts

## Dataset

Instacart Online Grocery Shopping Dataset 2017

https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis

## Example Analytics

The warehouse enables analysis such as:

- product reorder rates
- customer ordering behavior
- department purchasing trends
- shopping patterns by day and hour

## Future Improvements

- Docker containerization
- Airflow orchestration
- Cloud deployment (Azure)
- Dashboard layer (Power BI / Metabase)