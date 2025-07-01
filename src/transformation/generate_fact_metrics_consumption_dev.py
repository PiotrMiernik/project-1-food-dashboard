import pandas as pd
import zipfile
from pathlib import Path

# CONFIG
ZIP_PATH = Path("data/raw/FAO/FoodBalance/faostat_consumption.zip")
CSV_FILENAME_IN_ZIP = "FoodBalanceSheets_E_All_Data.csv"
PRODUCTS_MAPPING = {
    'Wheat and products': 'Wheat',
    'Rice and products': 'Rice',
    'Maize and products': 'Maize',
    'Potatoes and products': 'Potatoes',
    'Sweet potatoes': 'Potatoes',
    'Soyabeans': 'Soya'
}
METRIC_TYPE = 'consumption'

# READ DATA FROM ZIP
with zipfile.ZipFile(ZIP_PATH, 'r') as z:
    with z.open(CSV_FILENAME_IN_ZIP) as f:
        df_raw = pd.read_csv(f, encoding='utf-8')

# FILTER BY ELEMENT CODE = 5142 AND ELEMENT = 'Food'
df_filtered = df_raw[(df_raw['Element Code'] == 5142) & (df_raw['Element'] == 'Food')].copy()

# FILTER PRODUCTS OF INTEREST
df_filtered = df_filtered[df_filtered['Item'].isin(PRODUCTS_MAPPING.keys())].copy()
df_filtered['product_name'] = df_filtered['Item'].map(PRODUCTS_MAPPING)

# MELT YEAR COLUMNS (drop forecast and note columns)
year_cols = [col for col in df_filtered.columns if col.startswith('Y') and not col.endswith(('F', 'N'))]
df_melted = df_filtered.melt(id_vars=['Area', 'product_name'], value_vars=year_cols,
                             var_name='year_str', value_name='value')

# PARSE YEAR
df_melted['year'] = df_melted['year_str'].str.extract(r'Y(\d{4})').astype(int)

# ADD METRIC TYPE
df_melted['metric_type'] = METRIC_TYPE

# RENAME FOR CONSISTENCY
df_melted.rename(columns={'Area': 'country_name'}, inplace=True)

# LOAD DIMENSIONS
dim_country = pd.read_csv('data/transformed/dim_country.csv')
dim_product = pd.read_csv('data/transformed/dim_product.csv')
dim_date = pd.read_csv('data/transformed/dim_date.csv')
dim_date_filtered = dim_date[dim_date['month'] == 1][['date_id', 'year']]

# JOIN COUNTRY
df_consumption = df_melted.merge(dim_country[['country_id', 'country_name']], on='country_name', how='left')

# JOIN PRODUCT
df_consumption = df_consumption.merge(dim_product[['product_id', 'product_name']], on='product_name', how='left')

# JOIN DATE
df_consumption = df_consumption.merge(dim_date_filtered, on='year', how='left')

# SELECT FINAL COLUMNS
fact_metrics = df_consumption[['date_id', 'product_id', 'country_id', 'metric_type', 'value']].copy()
fact_metrics.dropna(subset=['value', 'country_id', 'product_id', 'date_id'], inplace=True)

# ADD PRIMARY KEY (fact_id)
fact_metrics.reset_index(drop=True, inplace=True)
fact_metrics['fact_id'] = fact_metrics.index + 1
fact_metrics = fact_metrics[['fact_id'] + [col for col in fact_metrics.columns if col != 'fact_id']]

# CONVERT TO INT
fact_metrics['country_id'] = fact_metrics['country_id'].astype(int)
fact_metrics['product_id'] = fact_metrics['product_id'].astype(int)
fact_metrics['date_id'] = fact_metrics['date_id'].astype(int)

# EXPORT and preview for testing
fact_metrics.to_csv('data/transformed/fact_metrics_consumption.csv', index=False)
print(fact_metrics.head())
