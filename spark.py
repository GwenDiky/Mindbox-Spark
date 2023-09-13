from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list

spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

products = spark.createDataFrame([
    (1, "Product A"),
    (2, "Product B"),
    (3, "Product C"),
], ["product_id", "product_name"])

categories = spark.createDataFrame([
    (1, "Category X"),
    (2, "Category Y"),
    (3, "Category Z"),
], ["category_id", "category_name"])

product_category = spark.createDataFrame([
    (1, 1),
    (1, 2),
    (2, 2),
    (3, 3),
], ["product_id", "category_id"])

product_category_join = product_category.join(
    products,
    product_category.product_id == products.product_id,
    "left"
).join(
    categories,
    product_category.category_id == categories.category_id,
    "left"
).select(
    products.product_name,
    categories.category_name
)

result = product_category_join.groupBy("product_name").agg(collect_list("category_name").alias("categories"))

result.show(truncate=False)
spark.stop()
