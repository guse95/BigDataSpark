import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Вытаскиваем реальные переменные из .env, которые проброшены в контейнер
db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
clickhouse_user = os.environ.get("CLICKHOUSE_USER")
clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD")
print(clickhouse_user)
print(clickhouse_password)

# ======================================================
# SPARK SESSION
# ======================================================
spark = SparkSession.builder \
    .appName("ClickHouse_Reports") \
    .getOrCreate()

# ======================================================
# POSTGRES CONFIG
# ======================================================
POSTGRES_URL = f"jdbc:postgresql://postgres:5432/{db_name}"

POSTGRES_PROPERTIES = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

# ======================================================
# CLICKHOUSE CONFIG
# ======================================================
# ИСПРАВЛЕНО: протокол ch:// вместо clickhouse:// для нового JDBC-драйвера
CLICKHOUSE_URL = "jdbc:ch://clickhouse:8123/default?ssl=false"

CLICKHOUSE_PROPERTIES = {
    "user": clickhouse_user,
    "password": clickhouse_password,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

# ======================================================
# LOAD STAR SCHEMA TABLES
# ======================================================
sales = spark.read.jdbc(url=POSTGRES_URL, table="public.sales", properties=POSTGRES_PROPERTIES)
customers = spark.read.jdbc(url=POSTGRES_URL, table="public.customers", properties=POSTGRES_PROPERTIES)
products = spark.read.jdbc(url=POSTGRES_URL, table="public.products", properties=POSTGRES_PROPERTIES)
stores = spark.read.jdbc(url=POSTGRES_URL, table="public.stores", properties=POSTGRES_PROPERTIES)
suppliers = spark.read.jdbc(url=POSTGRES_URL, table="public.suppliers", properties=POSTGRES_PROPERTIES)

# ======================================================
# 1. PRODUCT SALES REPORT
# ======================================================
product_sales = sales.join(
    products,
    sales.product_id == products.id
).groupBy(
    products.name,
    products.category
).agg(
    round(sum(sales.total_price), 2).alias("total_revenue"),
    sum(sales.quantity).alias("total_sales"),
    round(avg(products.rating), 2).alias("avg_rating"),
    sum(products.reviews).alias("total_reviews")
).select(
    products.name.alias("product_name"),
    products.category.alias("category"),
    col("total_revenue"),
    col("total_sales"),
    col("avg_rating"),
    col("total_reviews")
)

# ======================================================
# 2. CUSTOMER SALES REPORT
# ======================================================
customer_sales = sales.join(
    customers,
    sales.customer_id == customers.id
).groupBy(
    customers.email,
    customers.first_name,
    customers.last_name,
    customers.country
).agg(
    round(sum(sales.total_price), 2).alias("total_spent"),
    round(avg(sales.total_price), 2).alias("avg_check")
).select(
    customers.email.alias("customer_email"),
    concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
    customers.country.alias("country"),
    col("total_spent"),
    col("avg_check")
)

# ======================================================
# 3. TIME SALES REPORT
# ======================================================
time_sales = sales \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .groupBy("year", "month") \
    .agg(
        round(sum("total_price"), 2).alias("revenue"),
        round(avg("total_price"), 2).alias("avg_order"),
        count("*").alias("orders")
    )

# ======================================================
# 4. STORE SALES REPORT
# ======================================================
store_sales = sales.join(
    stores,
    sales.store_id == stores.id
).groupBy(
    stores.name,
    stores.city,
    stores.country
).agg(
    round(sum(sales.total_price), 2).alias("revenue"),
    round(avg(sales.total_price), 2).alias("avg_check"),
    count("*").alias("total_orders")
).select(
    stores.name.alias("store_name"),
    stores.city.alias("city"),
    stores.country.alias("country"),
    col("revenue"),
    col("avg_check"),
    col("total_orders")
)

# ======================================================
# 5. SUPPLIER SALES REPORT
# ======================================================
supplier_sales = sales \
    .join(products, sales.product_id == products.id) \
    .join(suppliers, products.supplier_id == suppliers.id) \
    .groupBy(suppliers.name, suppliers.country) \
    .agg(
        round(sum(sales.total_price), 2).alias("revenue"),
        round(avg(products.price), 2).alias("avg_product_price"),
        sum(sales.quantity).alias("total_sales")
    ) \
    .select(
        suppliers.name.alias("supplier_name"),
        suppliers.country.alias("country"),
        col("revenue"),
        col("avg_product_price"),
        col("total_sales")
    )

# ======================================================
# 6. PRODUCT QUALITY REPORT
# ======================================================
product_quality = sales \
    .join(products, sales.product_id == products.id) \
    .groupBy(products.name) \
    .agg(
        round(avg(products.rating), 2).alias("avg_rating"),
        sum(products.reviews).alias("reviews"),
        sum(sales.quantity).alias("sales_count")
    ) \
    .select(
        products.name.alias("product_name"),
        col("avg_rating"),
        col("reviews"),
        col("sales_count")
    )

# ======================================================
# WRITE REPORTS TO CLICKHOUSE
# ======================================================
reports = [
    (product_sales, "report_product_sales"),
    (customer_sales, "report_customer_sales"),
    (time_sales, "report_time_sales"),
    (store_sales, "report_store_sales"),
    (supplier_sales, "report_supplier_sales"),
    (product_quality, "report_product_quality")
]

for dataframe, table_name in reports:
    processed_df = dataframe.na.fill(0) # Избавляемся от null в числовых полях, если они есть

    processed_df.write \
        .format("jdbc") \
        .option("url", CLICKHOUSE_URL) \
        .option("dbtable", f"default.{table_name}") \
        .option("user", CLICKHOUSE_PROPERTIES["user"]) \
        .option("password", CLICKHOUSE_PROPERTIES["password"]) \
        .option("driver", CLICKHOUSE_PROPERTIES["driver"]) \
        .mode("append") \
        .save()

    print(f"-> {table_name} loaded successfully to MergeTree Engine")

print("ALL CLICKHOUSE REPORTS CREATED SUCCESSFULLY!")