import os
os.environ["JAVA_HOME"] = "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Homework6") \
    .getOrCreate()

# ============================================================
# Q1: Spark version
# ============================================================
print("=" * 60)
print(f"Q1: Spark version = {spark.version}")
print("=" * 60)

# ============================================================
# Q2: Repartition to 4, save as parquet, check average file size
# ============================================================
df = spark.read.parquet("data/yellow_tripdata_2025-11.parquet")

df.repartition(4).write.parquet("data/yellow_nov_2025_4parts", mode="overwrite")

import glob
files = glob.glob("data/yellow_nov_2025_4parts/part-*.parquet")
sizes = [os.path.getsize(f) for f in files]
avg_size_mb = sum(sizes) / len(sizes) / (1024 * 1024)
print(f"\nQ2: Average parquet file size = {avg_size_mb:.1f} MB")
print(f"    ({len(files)} files, sizes: {[round(s/1024/1024, 1) for s in sizes]} MB)")
print("=" * 60)

# ============================================================
# Q3: Count trips starting on November 15
# ============================================================
count_nov15 = df.filter(F.to_date("tpep_pickup_datetime") == "2025-11-15").count()
print(f"\nQ3: Trips on Nov 15 = {count_nov15}")
print("=" * 60)

# ============================================================
# Q4: Longest trip in hours
# ============================================================
df_with_duration = df.withColumn(
    "duration_hours",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
)
max_duration = df_with_duration.agg(F.max("duration_hours")).collect()[0][0]
print(f"\nQ4: Longest trip = {max_duration:.1f} hours")
print("=" * 60)

# ============================================================
# Q5: Spark UI port (knowledge question)
# ============================================================
print(f"\nQ5: Spark UI default port = 4040")
print("=" * 60)

# ============================================================
# Q6: Least frequent pickup location zone
# ============================================================
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)

pickup_counts = df.groupBy("PULocationID").count().withColumnRenamed("PULocationID", "LocationID")
joined = zones.join(pickup_counts, "LocationID", "left").fillna(0, subset=["count"])
least_frequent = joined.orderBy("count").select("Zone", "count").limit(10)

print(f"\nQ6: Least frequent pickup zones:")
least_frequent.show(10, truncate=False)
print("=" * 60)

spark.stop()
