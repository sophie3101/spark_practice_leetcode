from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("q180").getOrCreate()

import pandas as pd
data = [[1, 1], [2, 1], [3, 1], [4, 2], [5, 1], [6, 2], [7, 2]]
logs = spark.createDataFrame(pd.DataFrame(data, columns=['id', 'num']).astype({'id':'Int64', 'num':'Int64'}))

window_spec = Window.partitionBy("num").orderBy("id")
# Add row_number column
logs=logs.withColumn("row_number", sf.row_number().over(window_spec))
logs=logs.withColumn("diff", sf.col("id") -sf.col("row_number"))

logs.groupby("num","diff").agg(sf.count("*").alias("cnt")).filter(sf.col("cnt")>=3)
logs.groupby("num","diff")\
    .agg(sf.count("*").alias("cnt")).\
    filter(sf.col("cnt")>=3).\
    select(sf.col("num").alias("consecutiveNum")).\
    distinct().show()

# sql solution:
# WITH diff_cte AS 
# (select id,num,
# ROW_NUMBER()OVER(PARTITION BY num ORDER BY id),
# id - ROW_NUMBER()OVER(PARTITION BY num ORDER BY id) as diff
# FROM Logs
# )
# SELECT distinct num AS ConsecutiveNums 
# FROM diff_cte
# GROUP BY num, diff
# HAVING COUNT(diff)>=3