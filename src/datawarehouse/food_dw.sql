
-- Table: dim_date
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    all_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20),
    quarter INT
);

-- Table: dim_product
CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL
);

-- Table: dim_country
CREATE TABLE dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(255) NOT NULL,
    continent_name VARCHAR(100)
);

-- Table: fact_metrics (long version)
CREATE TABLE fact_metrics (
    fact_id SERIAL PRIMARY KEY,
    date_id INT NOT NULL,
    product_id INT NOT NULL,
    country_id INT NOT NULL,
    metric_type VARCHAR(50) NOT NULL, -- 'production', 'consumption', 'import', 'export'
    value DECIMAL(10, 2),
    population INT,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (country_id) REFERENCES dim_country(country_id)
);

-- Table: fact_prices
CREATE TABLE fact_prices (
    price_id SERIAL PRIMARY KEY,
    date_id INT NOT NULL,
    product_id INT NOT NULL,
    price_usd_per_ton DECIMAL(10,2),
    avg_annual_price DECIMAL(10,2),
    price_annual_change_pct DECIMAL(10,2),
    price_month_change_pct DECIMAL(10,2),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);

-- Indexes
CREATE INDEX idx_fact_metrics_date ON fact_metrics(date_id);
CREATE INDEX idx_fact_metrics_product ON fact_metrics(product_id);
CREATE INDEX idx_fact_metrics_country ON fact_metrics(country_id);
CREATE INDEX idx_fact_prices_date ON fact_prices(date_id);
CREATE INDEX idx_fact_prices_product ON fact_prices(product_id);

-- Table and column comments    

COMMENT ON TABLE dim_date IS 'Time dimension table (monthly aggregation)';
COMMENT ON COLUMN dim_date.date_id IS 'Unique month identifier (e.g., 202501 for January 2025)';
COMMENT ON TABLE dim_product IS 'Product dimension table (Maze, Potatoes, Rice, Soya, Wheat)';
COMMENT ON TABLE dim_country IS 'Countries and continents dimension table';
COMMENT ON TABLE fact_metrics IS 'Fact table with data on production, consumption, import, and export of products, as well as population for individual countries';
COMMENT ON TABLE fact_prices IS 'Fact table with product pricing data';
COMMENT ON COLUMN fact_prices.avg_annual_price IS 'Average annual product price in USD';
COMMENT ON COLUMN fact_prices.price_annual_change_pct IS 'Year-over-year percentage change in price';
COMMENT ON COLUMN fact_prices.price_month_change_pct IS 'Month-over-month percentage change in price';