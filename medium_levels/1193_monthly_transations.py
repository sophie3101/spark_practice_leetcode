from pyspark.sql import SparkSession
from pyspark.sql.functions  import date_format,sum,count,col, when
spark = SparkSession.builder.appName("q1193").getOrCreate()

import pandas as pd
data = [[121, 'US', 'approved', 1000, '2018-12-18'], [122, 'US', 'declined', 2000, '2018-12-19'], [123, 'US', 'approved', 2000, '2019-01-01'], [124, 'DE', 'approved', 2000, '2019-01-07']]
transactions = spark.createDataFrame(pd.DataFrame(data, columns=['id', 'country', 'state', 'amount', 'trans_date']).astype({'id':'Int64', 'country':'object', 'state':'object', 'amount':'Int64', 'trans_date':'datetime64[ns]'}))

#
transactions=transactions.withColumn("month", date_format("trans_date", "yyyy-MM"))

transactions.groupBy(["month", "country"])\
    .agg(
        count("*").alias("trans_count"),
        sum("amount").alias("trans_total_amount"),
        sum(when(col("state") == "approved", 1).otherwise(0)).alias("approved_count"),
        sum(when(col("state") == "approved", col("amount"))).alias("approved_amount")
    )\
    .show()