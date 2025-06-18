import pandas as pd

# CONFIGURATION
SOURCE_FILE = 'data/raw/WB/CMO-Historical-Data-Monthly.xlsx'
SHEET_NAME = 'Monthly Prices'

# READ AND CLEAN RAW DATA
# Skip the first 4 metadata rows
df_raw = pd.read_excel(SOURCE_FILE, sheet_name=SHEET_NAME, header=4)

# Drop the first row which contains unit symbols (e.g. $/ton)
df_raw.drop(index=0, inplace=True)

# Rename date column and strip extra whitespace
df_raw.rename(columns={df_raw.columns[0]: 'year_month'}, inplace=True)
df_raw.columns = df_raw.columns.str.strip()

# SELECT AND RENAME PRODUCTS
PRODUCT_MAPPING = {
    'Soybeans': 'Soya',
    'Maize': 'Maize',
    'Rice, Thai 5%': 'Rice',
    'Wheat, US HRW': 'Wheat'
}
selected_columns = ['year_month'] + list(PRODUCT_MAPPING.keys())
df_filtered = df_raw[selected_columns].copy()
df_filtered.rename(columns=PRODUCT_MAPPING, inplace=True)

# TRANSFORM TO LONG FORMAT
df_melted = df_filtered.melt(id_vars='year_month', var_name='product_name', value_name='price_usd_per_ton')

# PARSE DATE AND EXTRACT YEAR/MONTH
df_melted['date'] = pd.to_datetime(df_melted['year_month'].str.replace('M', ''), format='%Y%m')
df_melted['year'] = df_melted['date'].dt.year
df_melted['month'] = df_melted['date'].dt.month
df_melted.sort_values(by=['product_name', 'date'], inplace=True)

# CALCULATE AGGREGATE FEATURES
df_melted['avg_annual_price'] = df_melted.groupby(['product_name', 'year'])['price_usd_per_ton'].transform('mean')
df_melted['price_month_change_pct'] = df_melted.groupby('product_name')['price_usd_per_ton'].pct_change() * 100
df_melted['price_annual_change_pct'] = df_melted.groupby('product_name')['avg_annual_price'].pct_change() * 100

# LOAD DIMENSION TABLES
dim_product = pd.read_csv('data/transformed/dim_product.csv')
dim_date = pd.read_csv('data/transformed/dim_date.csv')

# JOIN product_id
df_melted = df_melted.merge(dim_product[['product_id', 'product_name']], on='product_name', how='left')

# JOIN date_id based on year and month
df_melted = df_melted.merge(dim_date[['date_id', 'year', 'month']], on=['year', 'month'], how='left')

# SELECT FINAL COLUMNS FOR fact_prices
fact_prices = df_melted[['date_id', 'product_id', 'price_usd_per_ton',
                         'avg_annual_price', 'price_annual_change_pct',
                         'price_month_change_pct']].copy()

# ADD PRIMARY KEY (price_id)
fact_prices.reset_index(drop=True, inplace=True)
fact_prices['price_id'] = fact_prices.index + 1
fact_prices = fact_prices[['price_id'] + [col for col in fact_prices.columns if col != 'price_id']]

# Round selected columns to 2 decimal places
fact_prices['avg_annual_price'] = fact_prices['avg_annual_price'].round(2)
fact_prices['price_annual_change_pct'] = fact_prices['price_annual_change_pct'].round(2)
fact_prices['price_month_change_pct'] = fact_prices['price_month_change_pct'].round(2)

# EXPORT TO CSV
fact_prices.to_csv('data/transformed/fact_prices.csv', index=False)
print(fact_prices.head())
