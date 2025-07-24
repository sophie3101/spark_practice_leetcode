from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("q1204").getOrCreate()

import pandas as pd
data = [[5, 'Alice', 250, 1], [4, 'Bob', 175, 5], [3, 'Alex', 350, 2], [6, 'John Cena', 400, 3], [1, 'Winston', 500, 6], [2, 'Marie', 200, 4]]
queue = spark.createDataFrame(pd.DataFrame(data, columns=['person_id', 'person_name', 'weight', 'turn']).astype({'person_id':'Int64', 'person_name':'object', 'weight':'Int64', 'turn':'Int64'}))

window_spec = Window().orderBy("turn")
queue.withColumn("total_weight", sf.sum("weight").over(window_spec))\
    .filter(sf.col("total_weight")<=1000)\
    .orderBy("turn", ascending=False)\
    .limit(1)\
    .select("person_name").show()

# sql solutions
# WITH cte AS (
# select person_name, weight, turn,
# SUM(weight)OVER(ORDER BY turn) as total_weight
# FROM Queue
# )

# SELECT person_name
# FROM cte
# WHERE total_weight <=1000
# ORDER BY turn DESC
# LIMIT 1