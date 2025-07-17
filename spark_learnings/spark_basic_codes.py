# create Spark session
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName("LearnSpark").getOrCreate()
#shuffle
spark.conf.set("spark.sql.shuffle.partitions"
,"5")
#  Reading a CSV file
df = spark.read.csv("data.csv", header=True, inferSchema=True)

df.show(5)  # Show first 5 rows

# Select columns
df.select("name", "age").show()

# Filter rows
df_filtered = df.filter(df.age > 25).show()

# Group by and aggregate
df.groupBy("department").avg("salary").show()

# SQL queries
df.createOrReplaceTempView("people")
spark.sql("SELECT name, age FROM people WHERE age > 25").show()


# Write to Parquet
df.write.parquet("output.parquet")
# Write to CSV
df.write.csv("output.csv", header=True)