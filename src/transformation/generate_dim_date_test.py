import pandas as pd
import datetime

# Parameters for date range (start and end year of data)
start_year = 1960
current_year = datetime.date.today().year
end_year = current_year + 1

# Generate list of first days of each month
date_range = pd.date_range(start=f'{start_year}-01-01',
                           end=f'{end_year}-12-31',
                           freq='MS')  # MS = Month Start

# Create DataFrame with full date information
dim_date = pd.DataFrame({
    'all_date': date_range
})

# Generate columns according to data warehouse schema
dim_date['year'] = dim_date['all_date'].dt.year
dim_date['month'] = dim_date['all_date'].dt.month
dim_date['month_name'] = dim_date['all_date'].dt.strftime('%B')
dim_date['quarter'] = dim_date['all_date'].dt.quarter

# Create surrogate key for date_id (simulating SERIAL PK in PostgreSQL)
dim_date.reset_index(inplace=True)
dim_date.rename(columns={'index': 'date_id'}, inplace=True)
dim_date['date_id'] += 1  # Make IDs start from 1

# Reorder columns according to target schema
dim_date = dim_date[['date_id', 'all_date', 'year', 'month', 'month_name', 'quarter']]

# Preview first rows
print(dim_date.head(15))
print(dim_date.tail(15))

# Export to CSV file
dim_date.to_csv('dim_date.csv', index=False)
