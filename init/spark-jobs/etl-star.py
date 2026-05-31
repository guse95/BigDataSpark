import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")


spark = SparkSession.builder \
    .appName("PetShop_ETL") \
    .getOrCreate()

jdbc_url = f"jdbc:postgresql://postgres:5432/{db_name}"

props = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}
df = spark.read.jdbc(
    url=jdbc_url,
    table="mock_data",
    properties=props
)
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

customers.write.jdbc(
    jdbc_url,
    "customers",
    mode="append",
    properties=props
)
sellers = df.select(
    col("seller_first_name").alias("first_name"),
    col("seller_last_name").alias("last_name"),
    col("seller_email").alias("email"),
    col("seller_country").alias("country"),
    col("seller_postal_code").alias("postal_code")
).dropDuplicates(["email"])

sellers.write.jdbc(
    jdbc_url,
    "sellers",
    mode="append",
    properties=props
)
suppliers = df.select(
    col("supplier_name").alias("name"),
    col("supplier_contact").alias("contact"),
    col("supplier_email").alias("email"),
    col("supplier_phone").alias("phone"),
    col("supplier_address").alias("location"),
    col("supplier_city").alias("city"),
    col("supplier_country").alias("country")
).dropDuplicates(["email"])

suppliers.write.jdbc(
    jdbc_url,
    "suppliers",
    mode="append",
    properties=props
)
stores = df.select(
    col("store_name").alias("name"),
    col("store_state").alias("state"),
    col("store_phone").alias("phone"),
    col("store_email").alias("email"),
    col("store_location").alias("location"),
    col("store_city").alias("city"),
    col("store_country").alias("country")
).dropDuplicates(["email"])

stores.write.jdbc(
    jdbc_url,
    "stores",
    mode="append",
    properties=props
)
customers_db = spark.read.jdbc(
    jdbc_url,
    "customers",
    properties=props
)

sellers_db = spark.read.jdbc(
    jdbc_url,
    "sellers",
    properties=props
)

suppliers_db = spark.read.jdbc(
    jdbc_url,
    "suppliers",
    properties=props
)

stores_db = spark.read.jdbc(
    jdbc_url,
    "stores",
    properties=props
)
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
    to_date(col("product_release_date"), "M/d/yyyy")
        .alias("released_at"),
    to_date(col("product_expiry_date"), "M/d/yyyy")
        .alias("expiring_at")
).dropDuplicates([
    "name",
     "brand",
     "price",
     "category",
     "pet_category",
     "color",
     "size"
])

products.write.jdbc(
    jdbc_url,
    "products",
    mode="append",
    properties=props
)
products_db = spark.read.jdbc(
    jdbc_url,
    "products",
    properties=props
)
sales = df \
    .join(customers_db,
          df.customer_email == customers_db.email) \
    .join(sellers_db,
          df.seller_email == sellers_db.email) \
    .join(stores_db,
          df.store_email == stores_db.email) \
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
        to_date(col("sale_date"), "M/d/yyyy")
            .alias("date"),

        customers_db.id.alias("customer_id"),
        sellers_db.id.alias("seller_id"),
        products_db.id.alias("product_id"),
        stores_db.id.alias("store_id"),

        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price")
    )

sales.write.jdbc(
    jdbc_url,
    "sales",
    mode="append",
    properties=props
)

print("ETL COMPLETED")