import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, round, year, month, concat_ws

# Читаем переменные окружения
db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")

clickhouse_db = os.environ.get("CLICKHOUSE_DB")
clickhouse_user = os.environ.get("CLICKHOUSE_USER")
clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD")

print(f"ClickHouse DB: {clickhouse_db}")
print(f"ClickHouse User: {clickhouse_user}")


def get_spark_session():
    return SparkSession.builder \
        .appName("ClickHouse_Reports") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.4.6") \
        .getOrCreate()


def read_from_postgres(spark, table_name):
    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://postgres:5432/{db_name}") \
        .option("dbtable", f"public.{table_name}") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()


def write_to_clickhouse(df, table_name):
    # Заменяем null на 0 в числовых полях перед записью
    processed_df = df.fillna(0)

    processed_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:ch://clickhouse:8123/{clickhouse_db}?compress=0") \
        .option("dbtable", table_name) \
        .option("user", clickhouse_user) \
        .option("password", clickhouse_password) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("batchsize", "1000") \
        .option("isolationLevel", "NONE") \
        .mode("overwrite") \
        .save()
    print(f"-> {table_name} loaded successfully to ClickHouse")


def create_reports():
    spark = get_spark_session()

    # 1. Чтение данных
    sales = read_from_postgres(spark, "sales")
    customers = read_from_postgres(spark, "customers")
    products = read_from_postgres(spark, "products")
    stores = read_from_postgres(spark, "stores")
    suppliers = read_from_postgres(spark, "suppliers")

    # 2. Расчет отчетов и их запись

    # REPORT 1: PRODUCT SALES
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
    write_to_clickhouse(product_sales, "report_product_sales")

    # REPORT 2: CUSTOMER SALES
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
    write_to_clickhouse(customer_sales, "report_customer_sales")

    # REPORT 3: TIME SALES
    time_sales = sales \
        .withColumn("year", year("date")) \
        .withColumn("month", month("date")) \
        .groupBy("year", "month") \
        .agg(
        round(sum("total_price"), 2).alias("revenue"),
        round(avg("total_price"), 2).alias("avg_order"),
        count("*").alias("orders")
    )
    write_to_clickhouse(time_sales, "report_time_sales")

    # REPORT 4: STORE SALES
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
    write_to_clickhouse(store_sales, "report_store_sales")

    # REPORT 5: SUPPLIER SALES
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
    write_to_clickhouse(supplier_sales, "report_supplier_sales")

    # REPORT 6: PRODUCT QUALITY
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
    write_to_clickhouse(product_quality, "report_product_quality")

    print("ALL CLICKHOUSE REPORTS CREATED SUCCESSFULLY!")
    spark.stop()


if __name__ == "__main__":
    create_reports()