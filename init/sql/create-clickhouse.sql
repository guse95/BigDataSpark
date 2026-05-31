CREATE TABLE IF NOT EXISTS report_product_sales
(
    product_name String,
    category String,
    total_revenue Float64,
    total_sales UInt64,
    avg_rating Float64,
    total_reviews UInt64
)
ENGINE = MergeTree()
ORDER BY product_name;

CREATE TABLE IF NOT EXISTS report_customer_sales
(
    customer_email String,
    customer_name String,
    country String,
    total_spent Float64,
    avg_check Float64
)
ENGINE = MergeTree()
ORDER BY customer_email;

CREATE TABLE IF NOT EXISTS report_time_sales
(
    year UInt32,
    month UInt32,
    revenue Float64,
    avg_order Float64,
    orders UInt64
)
ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS report_store_sales
(
    store_name String,
    city String,
    country String,
    revenue Float64,
    avg_check Float64,
    total_orders UInt64
)
ENGINE = MergeTree()
ORDER BY store_name;

CREATE TABLE IF NOT EXISTS report_supplier_sales
(
    supplier_name String,
    country String,
    revenue Float64,
    avg_product_price Float64,
    total_sales UInt64
)
ENGINE = MergeTree()
ORDER BY supplier_name;

CREATE TABLE IF NOT EXISTS report_product_quality
(
    product_name String,
    avg_rating Float64,
    reviews UInt64,
    sales_count UInt64
)
ENGINE = MergeTree()
ORDER BY product_name;