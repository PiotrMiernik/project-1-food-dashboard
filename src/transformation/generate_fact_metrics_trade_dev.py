import pandas as pd
import zipfile
from pathlib import Path

# CONFIG
ZIP_PATH = Path("data/raw/FAO/Trade/faostat_export_import.zip")
CSV_FILENAME_IN_ZIP = "Trade_CropsLivestock_E_All_Data_NOFLAG.csv"
OUTPUT_FILE = 'data/transformed/fact_metrics_trade.csv'

PRODUCTS_MAPPING = {
    'Wheat': 'Wheat',
    'Maize (corn)': 'Maize',
    'Green corn (maize)': 'Maize',
    'Rice': 'Rice',
    'Soya beans': 'Soya',
    'Potatoes': 'Potatoes'
}
METRIC_TYPE_MAP = {
    5610: 'import',  # Import quantity (t)
    5910: 'export'   # Export quantity (t)
}

# READ FROM ZIP
with zipfile.ZipFile(ZIP_PATH, 'r') as z:
    with z.open(CSV_FILENAME_IN_ZIP) as f:
        df_raw = pd.read_csv(f, encoding='utf-8')

# FILTER BY ELEMENT CODE (import/export)
df_filtered = df_raw[df_raw['Element Code'].isin(METRIC_TYPE_MAP.keys())].copy()

# FILTER PRODUCTS
df_filtered = df_filtered[df_filtered['Item'].isin(PRODUCTS_MAPPING.keys())].copy()
df_filtered['product_name'] = df_filtered['Item'].map(PRODUCTS_MAPPING)

# MAP METRIC TYPE
df_filtered['metric_type'] = df_filtered['Element Code'].map(METRIC_TYPE_MAP)

# MELT YEARS
year_cols = [col for col in df_filtered.columns if col.startswith('Y')]
df_melted = df_filtered.melt(id_vars=['Area', 'product_name', 'metric_type'], 
                             value_vars=year_cols,
                             var_name='year_str', value_name='value')

# PARSE YEAR
df_melted['year'] = df_melted['year_str'].str.extract(r'Y(\d{4})').astype(int)

# RENAME
df_melted.rename(columns={'Area': 'country_name'}, inplace=True)

# LOAD DIM TABLES
dim_country = pd.read_csv('data/transformed/dim_country.csv')
dim_product = pd.read_csv('data/transformed/dim_product.csv')
dim_date = pd.read_csv('data/transformed/dim_date.csv')
dim_date_filtered = dim_date[dim_date['month'] == 1][['date_id', 'year']]

# MERGE
df_trade = df_melted.merge(dim_country[['country_id', 'country_name']], on='country_name', how='left')
df_trade = df_trade.merge(dim_product[['product_id', 'product_name']], on='product_name', how='left')
df_trade = df_trade.merge(dim_date_filtered, on='year', how='left')

# FINAL FORMAT
fact_metrics = df_trade[['date_id', 'product_id', 'country_id', 'metric_type', 'value']].copy()
fact_metrics.dropna(subset=['value', 'country_id', 'product_id', 'date_id'], inplace=True)

# ADD PRIMARY KEY
fact_metrics.reset_index(drop=True, inplace=True)
fact_metrics['fact_id'] = fact_metrics.index + 1
fact_metrics = fact_metrics[['fact_id'] + [col for col in fact_metrics.columns if col != 'fact_id']]

# CONVERT ID COLUMNS TO INT
fact_metrics['country_id'] = fact_metrics['country_id'].astype(int)
fact_metrics['product_id'] = fact_metrics['product_id'].astype(int)
fact_metrics['date_id'] = fact_metrics['date_id'].astype(int)

# EXPORT
fact_metrics.to_csv(OUTPUT_FILE, index=False)
print(fact_metrics.head())
