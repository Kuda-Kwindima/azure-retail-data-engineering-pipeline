# Instacart Retail Analytics Warehouse Pipeline

This project builds an end-to-end data engineering pipeline that transforms the Instacart Online Grocery Shopping dataset into a structured PostgreSQL analytics warehouse.

The pipeline loads raw CSV data, cleans and models it, and produces analytics-ready data marts for business insights.


## Pipeline Architecture

The pipeline ingests raw Instacart CSV files, loads them into a PostgreSQL warehouse, and transforms them through layered schemas (raw → staging → warehouse → marts) orchestrated with Prefect.

```mermaid
flowchart TD

A[Instacart CSV Files] --> B[Python Ingestion<br>src/load_raw.py]

B --> C[PostgreSQL RAW Layer]

C --> D[STAGING Transformations]

D --> E[WAREHOUSE Dimensional Model]

E --> F[DATA MARTS]

F --> G[Analytics Queries]

subgraph Orchestration
H[Prefect Flow]
end

H --> B
```

## Technologies

- Python
- PostgreSQL
- SQLAlchemy
- Prefect
- Docker
- Pandas
- Dimensional Modeling

### Examples of business questions answered:

- Which products have the highest reorder probability?
- What time of day do customers shop most?
- Which departments drive the most repeat purchases?




## Project Structure

```text
config/
data/
flows/
sql/
    raw/
    staging/
    warehouse/
    marts/
src/
tests/
README.md
requirements.txt
```
---

## Data Model

The warehouse follows a dimensional model with a central fact table for order items and supporting dimension tables.

```mermaid
erDiagram

fact_order_items {
    int order_id
    int product_id
    int add_to_cart_order
    int reordered
}

dim_orders {
    int order_id
    int user_id
    int order_number
    int order_dow
    int order_hour_of_day
}

dim_products {
    int product_id
    string product_name
    int aisle_id
    int department_id
}

dim_aisles {
    int aisle_id
    string aisle
}

dim_departments {
    int department_id
    string department
}

fact_order_items }o--|| dim_orders : order_id
fact_order_items }o--|| dim_products : product_id
dim_products }o--|| dim_aisles : aisle_id
dim_products }o--|| dim_departments : department_id
```
---

## Pipeline Steps

1. Create raw tables
2. Load raw CSV data
3. Transform data into staging tables
4. Build dimensional warehouse tables
5. Generate analytics marts

## Pipeline scale

```
Processes 30M+ order-item records into a dimensional warehouse.
```

---

## Dataset

Instacart Online Grocery Shopping Dataset 2017

https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis

---

## Example Analytics

The warehouse enables analysis such as:

- product reorder rates
- customer ordering behavior
- department purchasing trends
- shopping patterns by day and hour

---

## Running the Pipeline

1. Install dependencies

```
pip install -r requirements.txt
```

2. Configure database credentials

```
Create a `.env` file using `.env.example`.
```

3. Run the pipeline

```
python flows/instacart_flow.py
```
4. Using Make

```
make install
make run
```

## Pipeline Orchestration

The pipeline is orchestrated using Prefect, allowing monitoring of flow runs and task execution.

![Prefect Dashboard](docs/prefect_dashboard.png)

## Docker Pipeline Execution

The pipeline runs in Docker containers for reproducible local execution.

- PostgreSQL warehouse container
- Pipeline execution container

![Docker Pipeline](docs/docker_pipeline.png)

## Future Improvements

- Airflow orchestration
- Cloud deployment (Azure)
- Dashboard layer (Power BI / Metabase)