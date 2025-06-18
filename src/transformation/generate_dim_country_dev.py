import pandas as pd

# Source file location (FAOSTAT - raw data)
SOURCE_FILE = 'data/raw/FAO/Value_of_Production/Value_of_Production_E_All_Data.csv'

# External mapping file for continents
MAPPING_FILE = 'data/resources/m49_continents.csv'

# Load source data
df_raw = pd.read_csv(SOURCE_FILE, encoding='utf-8')

# Select only columns related to countries
df_countries = df_raw[['Area Code (M49)', 'Area']].drop_duplicates()

# FAO provides codes like "'004" → remove apostrophe, cast as string, pad with zeros
df_countries['m49_code'] = (
    df_countries['Area Code (M49)']
    .astype(str)
    .str.replace("'", "", regex=False)
    .str.zfill(3)
)

# Load external mapping table (M49 → Continent) with m49_code column as string
df_mapping = pd.read_csv(MAPPING_FILE, dtype={'m49_code': str})

# Merge to enrich with continent names
df_enriched = pd.merge(df_countries, df_mapping, on='m49_code', how='left')

# Reset index to create simple incremental surrogate key (simulate SERIAL PK in SQL)
df_enriched.reset_index(drop=True, inplace=True)
df_enriched['country_id'] = df_enriched.index + 1

# Select final columns according to warehouse schema
dim_country = df_enriched[['country_id', 'Area', 'continent_name']]
dim_country.rename(columns={'Area': 'country_name'}, inplace=True)

# Export transformed file
dim_country.to_csv('data/transformed/dim_country.csv', index=False)

# Preview result
print(dim_country.head(10))
print(f"\nTotal countries processed: {len(dim_country)}")

