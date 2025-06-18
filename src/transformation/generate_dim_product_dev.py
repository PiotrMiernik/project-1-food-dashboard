import pandas as pd

# Source file location
SOURCE_FILE = 'data/raw/FAO/Value_of_Production/Value_of_Production_E_All_data.csv'

# Dictionary of products of interest    
PRODUCTS_OF_INTREST = {
    "Wheat": "Wheat",
    "Maize (corn)": "Maize",
    "Rice": "Rice",
    "Soya beans": "Soya",
    "Potatoes": "Potatoes"
}

# Read data from source file
df_raw = pd.read_csv(SOURCE_FILE, encoding='utf-8')

# Select and filter relevant columns
df_products = df_raw[['Item Code', 'Item']].drop_duplicates()

# Filter products of interest
df_filtered = df_products[df_products['Item'].isin(PRODUCTS_OF_INTREST.keys())].copy()

#Procucts name mapping
df_filtered['product_name'] = df_filtered['Item'].map(PRODUCTS_OF_INTREST)

# Create surrogate key for product_id (simulating SERIAL PK in PostgreSQL)
df_filtered.reset_index(drop=True, inplace=True)
df_filtered['product_id'] = df_filtered.index + 1  # start from 1

# Reorder columns according to target table schema
dim_product = df_filtered[['product_id', 'product_name']]

# Preview
print(dim_product)

# Save to CSV
dim_product.to_csv('data/transformed/dim_product.csv', index=False)







