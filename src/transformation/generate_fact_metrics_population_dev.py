import pandas as pd
import zipfile
from pathlib import Path

# CONFIG
ZIP_PATH = Path("data/raw/WB/wb_population.zip")
CSV_FILENAME_IN_ZIP = "API_SP.POP.TOTL_DS2_en_csv_v2_127006.csv"
OUTPUT_FILE = "data/transformed/fact_metrics_population.csv"
METRIC_TYPE = "population"
TECHNICAL_PRODUCT_ID = 0  # required by schema

# READ CSV FROM ZIP (skip first 4 rows, header is in row 5)
with zipfile.ZipFile(ZIP_PATH, 'r') as z:
    with z.open(CSV_FILENAME_IN_ZIP) as f:
        df_raw = pd.read_csv(f, skiprows=4)

# KEEP ONLY RELEVANT COLUMNS
df_filtered = df_raw[['Country Name'] + [str(year) for year in range(1960, 2025)]].copy()
df_filtered.rename(columns={'Country Name': 'country_name'}, inplace=True)

# MELT WIDE → LONG
df_melted = df_filtered.melt(id_vars='country_name', var_name='year', value_name='value')
df_melted['year'] = df_melted['year'].astype(int)
df_melted['metric_type'] = METRIC_TYPE
df_melted['product_id'] = TECHNICAL_PRODUCT_ID

# LOAD DIM TABLES
dim_country = pd.read_csv('data/transformed/dim_country.csv')
dim_date = pd.read_csv('data/transformed/dim_date.csv')
dim_date_filtered = dim_date[dim_date['month'] == 1][['date_id', 'year']]

# JOIN COUNTRY
df_metrics = df_melted.merge(dim_country[['country_id', 'country_name']], on='country_name', how='left')

# JOIN DATE
df_metrics = df_metrics.merge(dim_date_filtered, on='year', how='left')

# SELECT FINAL COLUMNS
fact_metrics = df_metrics[['date_id', 'product_id', 'country_id', 'metric_type', 'value']].copy()
fact_metrics.dropna(subset=['value', 'country_id', 'date_id'], inplace=True)

# ADD fact_id
fact_metrics.reset_index(drop=True, inplace=True)
fact_metrics['fact_id'] = fact_metrics.index + 1
fact_metrics = fact_metrics[['fact_id'] + [col for col in fact_metrics.columns if col != 'fact_id']]

# CONVERT TYPES
fact_metrics['country_id'] = fact_metrics['country_id'].astype(int)
fact_metrics['date_id'] = fact_metrics['date_id'].astype(int)
fact_metrics['value'] = fact_metrics['value'].astype(int)

# EXPORT
fact_metrics.to_csv(OUTPUT_FILE, index=False)
print(fact_metrics.head())
print(f"\nTotal rows: {len(fact_metrics)}")
