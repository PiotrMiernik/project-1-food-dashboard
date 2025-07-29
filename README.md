[![Run Pytest on Push and PR](https://github.com/PiotrMiernik/project-1-food-dashboard/actions/workflows/run-tests.yml/badge.svg)](https://github.com/PiotrMiernik/project-1-food-dashboard/actions/workflows/run-tests.yml)

[![Validate Transformed Data](https://github.com/PiotrMiernik/project-1-food-dashboard/actions/workflows/validate-transformed.yml/badge.svg)](https://github.com/PiotrMiernik/project-1-food-dashboard/actions/workflows/validate-transformed.yml)

# Food Dashboard Project

## Goal

Build a complete ETL data pipeline for processing global food production, consumption, trade, and pricing data (for top 5 food products). The pipeline loads transformed data into an AWS RDS PostgreSQL data warehouse and supports visual analytics in Tableau.

## Technologies

- **Cloud:** AWS S3 (data lake), Lambda, RDS (PostgreSQL), CloudWatch, SNS, Event Bridge, Budgets
- **Python libraries:** pandas, boto3, psycopg2, requests, pytest, moto, openpyxl, xlsxwriter
- **BI tool:** Tableau Desktop/ Public
- **Version control:** GitHub (with production/dev branches), GitHub Actions (CI/CD)

## Project Architecture

- Data sources: FAO, World Bank
- Raw storage: AWS S3 data lake with raw and transformed zones (buckets)
- ETL Processing: Python scripts (pandas), AWS Lambda (modular Lambda-based scripts)
- Data Warehouse: AWS RDS (PostgreSQL)
- Visualization: Tableau dashboard

## Repository structure

├── .github/workflows/       # CI/CD workflows for automated testing and validation (GitHub Actions)
├── data/                    # Local datasets (raw, transformed, additional resources for testing)
│   ├── raw/                 # Raw source data
│   ├── resources/           # Additional datasets (e.g. continent mappings)
│   └── transformed/         # Transformed data used for testing and validation
├── diagrams/                # Project architecture and data warehouse diagrams
├── lambda_layer/            # AWS Lambda Layer code and dependencies for reusable packaging
├── src/                     # Source code for ETL pipeline and supporting modules
│   ├── extraction/          # Scripts for downloading raw data from external sources (FAO, World Bank)
│   ├── transformation/      # Scripts for transforming raw data into structured format
│   ├── load/                # Scripts for loading transformed data into AWS RDS (PostgreSQL) data warehouse
│   ├── datawarehouse/       # SQL scripts for building the data warehouse schema
│   ├── helpers/             # Reusable utility modules (e.g. s3_utils, db_utils, validation.py)
│   └── validation/          # Script to run data validation functions on AWS using helper modules
├── tests/                   # Unit tests for transformation, load, and helper functions
├── .gitignore               # Git ignore rules for unnecessary files and folders
├── requirements.txt         # Python dependencies for running the project
├── README.md                # Project overview, setup instructions, and documentation
└── LICENSE                  # Project license and usage rules

## Key Features

- Modular Python ETL pipeline using AWS Lambda
- Separation of raw and transformed zones in S3
- Production-ready dimension and fact tables
- Helper modules for S3 and RDS operations
- Prepared for unit and data validation tests

 **Note**: This project uses environment variables in AWS Lambda for credentials and configuration, avoiding hardcoded secrets or config files in version control.
