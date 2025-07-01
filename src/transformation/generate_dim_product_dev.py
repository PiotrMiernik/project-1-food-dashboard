import pandas as pd
import zipfile
from pathlib import Path

# Source ZIP file and internal CSV filename
ZIP_PATH = Path("data/raw/FAO/Value_of_Production/faostat_production.zip")
CSV_FILENAME_IN_ZIP = "Value_of_Production_E_All_Data.csv"

# Dictionary of products of interest
PRODUCTS_OF_INTEREST = {
    "Wheat": "Wheat",
    "Maize (corn)": "Maize",
    "Rice": "Rice",
    "Soya beans": "Soya",
    "Potatoes": "Potatoes"
}

# Read CSV directly from ZIP archive
with zipfile.ZipFile(ZIP_PATH, 'r') as z:
    with z.open(CSV_FILENAME_IN_ZIP) as f:
        df_raw = pd.read_csv(f, encoding='utf-8')

# Select and filter relevant columns
df_products = df_raw[['Item Code', 'Item']].drop_duplicates()

# Filter products of interest
df_filtered = df_products[df_products['Item'].isin(PRODUCTS_OF_INTEREST.keys())].copy()
df_filtered['product_name'] = df_filtered['Item'].map(PRODUCTS_OF_INTEREST)

# Remove unnecessary column
df_filtered = df_filtered[['product_name']].drop_duplicates()

# Add technical row for population metric
technical_row = pd.DataFrame([{'product_name': 'N/A'}])
df_final = pd.concat([technical_row, df_filtered], ignore_index=True)

# Assign product_id starting from 0
df_final.reset_index(inplace=True)
df_final.rename(columns={'index': 'product_id'}, inplace=True)

# Reorder columns
dim_product = df_final[['product_id', 'product_name']]

# Preview
print(dim_product)

# Save to CSV
dim_product.to_csv('data/transformed/dim_product.csv', index=False)

