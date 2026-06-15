from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("create-table") \
    .enableHiveSupport() \
    .getOrCreate()

# Читаем ваш CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://gorodokbucket/2026/06/15/used_cars/")

# Пишем в формате Parquet
df.write.mode("overwrite") \
    .parquet("s3a://gorodokbucket/used_cars_parquet/")

spark.stop()
