import pandas as pd
import os

# 1. Generic validation functions

def check_schema(df, expected_schema):
    actual_cols = list(df.columns)
    if actual_cols != expected_schema:
        raise ValueError(f"Invalid schema. Expected: {expected_schema}, got: {actual_cols}")
    
def check_nulls(df, required_columns):
    nulls = df[required_columns].isnull().sum()
    if nulls.any():
        raise ValueError(f"Missing values in columns {nulls[nulls > 0].to_dict()}")
    
def check_unique(df, subset):
    if not df[subset].is_unique:
        raise ValueError(f"Duplicate values found in column(s): {subset}")
    
def check_value_ranges(df, column_range: dict):
    for col, (min_val, max_val) in column_range.items():
        if not df[col].between(min_val, max_val).all():
            raise ValueError(f"Values in column {col} are out of expected range: ({min_val} to {max_val})")
        
def check_allowed_values(df, allowed_values: dict):
    for col, allowed in allowed_values.items():
        if not df[col].isin(allowed).all():
            raise ValueError(f"Column '{col}' contains unexpected values: {set(df[col]) - set(allowed)}")
        
def check_row_count(df):
    if df.shape[0] == 0:
        raise ValueError("The dataset is empty and it ain't right, you feel me homie? (I watched too much The Wire lately!)")
    
def check_duplicates(df):
    if df.duplicated().any():
        raise ValueError("Duplicate rows found in the dataset.")
    
def check_date_range(df, column, min_date, max_date):
    df[column] = pd.to_datetime(df[column])
    if not df[column].between(min_date, max_date).all():
        raise ValueError(f"Dates in column '{column}' are outside the expected range ({min_date}, {max_date})")
    
# 2. Dataset-specific validation functions

def validate_dim_country(df):
    check_schema(df, ["country_id", "country_name", "continent_name"])
    check_nulls(df, ["country_id", "country_name"])
    check_unique(df, "country_id")
    check_row_count(df)
    check_duplicates(df)

def validate_dim_date(df):
    check_schema(df, ["date_id", "all_date", "year", "month", "month_name", "quarter"])
    check_nulls(df, ["date_id", "all_date", "year", "month"])
    check_unique(df, "date_id")
    check_row_count(df)
    check_duplicates(df)
    check_date_range(df, "all_date", "1960-01-01", "2026-12-31")

def validate_dim_product(df):
    check_schema(df, ["product_id", "product_name"])
    check_nulls(df, ["product_id"])
    check_unique(df, "product_id")
    check_row_count(df)
    check_duplicates(df)

    # Allow null or "N/A" only if product_id == 0
    invalid_rows = df[(df["product_id"] != 0) & (df["product_name"].isnull())]
    if not invalid_rows.empty:
        raise ValueError(f"Missing product_name for product_id(s): {invalid_rows['product_id'].tolist()}")

    # Replace null with 'N/A' ONLY for product_id == 0
    df.loc[(df["product_id"] == 0) & (df["product_name"].isnull()), "product_name"] = "N/A"

    check_allowed_values(df, {"product_name": ["N/A", "Maize", "Potatoes", "Rice", "Soya", "Wheat"]})

def validate_fact_prices(df):
    check_schema(df, [
        "price_id", "date_id", "product_id",
        "price_usd_per_ton", "avg_annual_price",
        "price_annual_change_pct", "price_month_change_pct"
    ])
    check_nulls(df, ["price_id", "date_id", "product_id", "price_usd_per_ton", "avg_annual_price"])
    check_unique(df, "price_id")
    check_row_count(df)
    check_duplicates(df)
    check_value_ranges(df, {"price_usd_per_ton": (0, float("inf")), "avg_annual_price": (0, float("inf"))})

def validate_fact_metrics(df):
    check_schema(df, ["fact_id", "date_id", "product_id", "country_id", "metric_type", "value"])
    check_nulls(df, ["fact_id", "date_id", "product_id", "country_id", "metric_type"])
    check_unique(df, "fact_id")
    check_row_count(df)
    check_duplicates(df)
    check_allowed_values(df, {"metric_type": ["production", "consumption", "import", "export", "population"]})
    check_value_ranges(df, {"value": (0, float("inf"))})

# 3. Main validation runner

def run_all_validations():
    path = "data/transformed"
    validators = {
        "dim_country.csv":validate_dim_country,
        "dim_date.csv":validate_dim_date,
        "dim_product.csv":validate_dim_product,
        "fact_prices.csv":validate_fact_prices,
        "fact_metrics.csv":validate_fact_metrics
    }
    
    for filename, validate_fn in validators.items():
        full_path = os.path.join(path, filename)
        print(f"Validating {filename}...")
        df = pd.read_csv(full_path)
        validate_fn(df) 
        print(f"Validation for {filename} completed successfully.")

if __name__ == "__main__":
    run_all_validations()