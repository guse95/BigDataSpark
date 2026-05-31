CREATE TABLE IF NOT EXISTS report_product_sales
(
    product_name String,
    category String,
    total_revenue Float64,
    total_sales Int64,
    avg_rating Float64,
    total_reviews Int64
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
    year Int32,
    month Int32,
    revenue Float64,
    avg_order Float64,
    orders Int64
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
    total_orders Int64
)
ENGINE = MergeTree()
ORDER BY store_name;

CREATE TABLE IF NOT EXISTS report_supplier_sales
(
    supplier_name String,
    country String,
    revenue Float64,
    avg_product_price Float64,
    total_sales Int64
)
ENGINE = MergeTree()
ORDER BY supplier_name;

CREATE TABLE IF NOT EXISTS report_product_quality
(
    product_name String,
    avg_rating Float64,
    reviews Int64,
    sales_count Int64
)
ENGINE = MergeTree()
ORDER BY product_name;