[![Run Pytest on Push and PR](https://github.com/PiotrMiernik/project-1-food-dashboard/actions/workflows/run-tests.yml/badge.svg)](https://github.com/PiotrMiernik/project-1-food-dashboard/actions/workflows/run-tests.yml)

# Food Dashboard Project

## Goal

Build a complete ETL data pipeline for processing global food production, consumption, trade, and pricing data (for top 5 food products). The pipeline loads transformed data into an AWS RDS PostgreSQL data warehouse and supports visual analytics in Tableau.

## Technologies

- **Cloud:** AWS S3 (data lake), AWS Lambda, AWS RDS (PostgreSQL), AWS
  CloudWatch
- **Python libraries:** pandas, boto3, psycopg2, requests, pytest, moto, openpyxl, xlsxwriter
- **BI tool:** Tableau
- **Version control:** GitHub (with production/dev branches), GitHub Actions (CI/CD)

## Project Architecture

- Data sources: FAO, World Bank
- Raw storage: AWS S3 data lake with raw and transformed zones (buckets)
- ETL Processing: Python scripts (pandas), AWS Lambda (modular Lambda-based scripts)
- Data Warehouse: AWS RDS (PostgreSQL)
- Visualization: Tableau dashboard

## Repository structure

├── data/                 # Local datasets (raw + transformed for testing + additional continents dataset)

│   ├── raw/

    ├── resources/

│   └── transformed/

├── diagrams/             # Project architecture diagrams

├── src/                  # Source code (ETL + helpers)

│   ├── extraction/          # Scripts for downloading raw data

│   ├── transformation/        # Scripts for transforming raw data into structured format

│   ├── load/             # Scripts for loading data into AWS RDS

│   ├── datawarehouse/              # SQL for building the data warehouse schema

│   └── helpers/          # Reusable helper modules (e.g. s3_utils, db_utils)

├── tests/                # Test cases (to be added)

├── .gitignore            # Git ignore rules

├── requirements.txt      # Project dependencies

└── README.md             # Project overview

└── LICENSE            # Usage rools

## Key Features

- Modular Python ETL pipeline using AWS Lambda
- Separation of raw and transformed zones in S3
- Production-ready dimension and fact tables
- Helper modules for S3 and RDS operations
- Prepared for unit and data validation tests

 **Note**: This project uses environment variables in AWS Lambda for credentials and configuration, avoiding hardcoded secrets or config files in version control.
