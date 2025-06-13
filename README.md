# Food Dashboard Project

## Goal

End-to-end data pipeline for processing food production, consumption, export and import and price data, stored in AWS RDS datawarehouse, with final dashboard in Tableau.

## Technologies

- AWS (S3, Lambda, RDS PostgreSQL, CloudWatch)
- Python (Pandas, SQLAlchemy, Boto3, Requests)
- Tableau (for BI dashboards)
- GitHub (for version control)

## Project Architecture

- Data sources: FAO, World Bank
- Raw storage: AWS S3 data lake - raw and transformed zones (buckets)
- ETL Processing: Python scripts (pandas), AWS Lambda
- Data Warehouse: AWS RDS (PostgreSQL)
- Visualization: Tableau dashboard

### Repository structure

- `config/` — config files (S3 buckets, RDS credentials, etc.)
- `data/` — local test datasets (sample raw and transformed files)
- `src/` — main source code: sql data warehouse creation, extraction, transformation, loaders
- `diagrams/` — project architecture diagram
- `tests/` — unit tests

---
