import pandas as pd
from pathlib import Path

# Path to the directory with transformed source files
TRANSFORMED_DIR = Path("data/transformed")

# Source files
files = {
    "consumption": TRANSFORMED_DIR / "fact_metrics_consumption.csv",
    "production": TRANSFORMED_DIR / "fact_metrics_production.csv",
    "trade": TRANSFORMED_DIR / "fact_metrics_trade.csv",
    "population": TRANSFORMED_DIR / "fact_metrics_population.csv"
}

# Final columns required by data warehouse schema
TARGET_COLUMNS = [
    "date_id",
    "product_id",
    "country_id",
    "metric_type",
    "value"
]

# DataFrames to be merged
frames = []

for metric_type, path in files.items():
    df = pd.read_csv(path)

    # Ensure all required columns exist
    if "value" not in df.columns:
        df["value"] = pd.NA

    # Keep only required columns
    df = df[TARGET_COLUMNS]
    frames.append(df)

# Concatenate all fact metric types into one table
fact_metrics = pd.concat(frames, ignore_index=True)

# Drop rows with missing key dimensions
fact_metrics.dropna(subset=["date_id", "product_id", "country_id", "metric_type"], inplace=True)

# Add surrogate key
fact_metrics.reset_index(drop=True, inplace=True)
fact_metrics["fact_id"] = fact_metrics.index + 1

# Reorder columns to match target schema
fact_metrics = fact_metrics[["fact_id"] + TARGET_COLUMNS]

# Export final fact_metrics table
output_path = TRANSFORMED_DIR / "fact_metrics.csv"
fact_metrics.to_csv(output_path, index=False)

# Preview
print(fact_metrics.head())
print(f"\nTotal rows in final fact_metrics: {len(fact_metrics)}")
