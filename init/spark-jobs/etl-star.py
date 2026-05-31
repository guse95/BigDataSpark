import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Читаем переменные окружения для Postgres
db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")

JDBC_URL = f"jdbc:postgresql://postgres:5432/{db_name}"

def get_spark_session():
    # Автоматически подтягиваем JAR-пакет для Postgres, как в рабочем примере
    return SparkSession.builder \
        .appName("PetShop_ETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def read_from_postgres(spark, table_name):
    return spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table_name) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def write_to_postgres(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table_name) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print(f"-> Table '{table_name}' appended successfully.")

def run_etl():
    spark = get_spark_session()

    # 1. Читаем сырые данные из mock_data
    df = read_from_postgres(spark, "mock_data")

    # 2. Обработка и запись CUSTOMERS
    customers = df.select(
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code"),
        col("pet_category"),
        col("customer_pet_name").alias("pet_name"),
        col("customer_pet_breed").alias("pet_breed")
    ).dropDuplicates(["email"])
    write_to_postgres(customers, "customers")

    # 3. Обработка и запись SELLERS
    sellers = df.select(
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code")
    ).dropDuplicates(["email"])
    write_to_postgres(sellers, "sellers")

    # 4. Обработка и запись SUPPLIERS
    suppliers = df.select(
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("location"),
        col("supplier_city").alias("city"),
        col("supplier_country").alias("country")
    ).dropDuplicates(["email"])
    write_to_postgres(suppliers, "suppliers")

    # 5. Обработка и запись STORES
    stores = df.select(
        col("store_name").alias("name"),
        col("store_state").alias("state"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_country").alias("country")
    ).dropDuplicates(["email"])
    write_to_postgres(stores, "stores")

    # 6. Читаем сохраненные справочники из БД для получения сгенерированных id
    customers_db = read_from_postgres(spark, "customers")
    sellers_db = read_from_postgres(spark, "sellers")
    suppliers_db = read_from_postgres(spark, "suppliers")
    stores_db = read_from_postgres(spark, "stores")

    # 7. Обработка и запись PRODUCTS
    products = df.join(
        suppliers_db,
        df.supplier_email == suppliers_db.email
    ).select(
        col("product_name").alias("name"),
        suppliers_db.id.alias("supplier_id"),
        col("product_category").alias("category"),
        col("pet_category"),
        col("product_brand").alias("brand"),
        col("product_price").alias("price"),
        col("product_quantity").alias("quantity"),
        col("product_weight").alias("weight"),
        col("product_color").alias("color"),
        col("product_size").alias("size"),
        col("product_material").alias("material"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        to_date(col("product_release_date"), "M/d/yyyy").alias("released_at"),
        to_date(col("product_expiry_date"), "M/d/yyyy").alias("expiring_at")
    ).dropDuplicates([
        "name", "brand", "price", "category", "pet_category", "color", "size"
    ])
    write_to_postgres(products, "products")

    # 8. Перечитываем продукты из БД для получения их id
    products_db = read_from_postgres(spark, "products")

    # 9. Обработка и запись SALES (Финальная таблица фактов)
    sales = df \
        .join(customers_db, df.customer_email == customers_db.email) \
        .join(sellers_db, df.seller_email == sellers_db.email) \
        .join(stores_db, df.store_email == stores_db.email) \
        .join(
            products_db,
            (
                (df.product_name == products_db.name) &
                (df.product_brand == products_db.brand) &
                (df.product_category == products_db.category) &
                (df.pet_category == products_db.pet_category) &
                (df.product_color == products_db.color) &
                (df.product_size == products_db.size)
            )
        ) \
        .select(
            to_date(col("sale_date"), "M/d/yyyy").alias("date"),
            customers_db.id.alias("customer_id"),
            sellers_db.id.alias("seller_id"),
            products_db.id.alias("product_id"),
            stores_db.id.alias("store_id"),
            col("sale_quantity").alias("quantity"),
            col("sale_total_price").alias("total_price")
        )
    write_to_postgres(sales, "sales")

    print("ETL COMPLETED")
    spark.stop()

if __name__ == "__main__":
    run_etl()