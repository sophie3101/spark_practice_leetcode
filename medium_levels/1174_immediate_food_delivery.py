
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf 
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("q1174").getOrCreate()

import pandas as pd 
data = [[1, 1, '2019-08-01', '2019-08-02'], [2, 2, '2019-08-02', '2019-08-02'], [3, 1, '2019-08-11', '2019-08-12'], [4, 3, '2019-08-24', '2019-08-24'], [5, 3, '2019-08-21', '2019-08-22'], [6, 2, '2019-08-11', '2019-08-13'], [7, 4, '2019-08-09', '2019-08-09']]
delivery = spark.createDataFrame(pd.DataFrame(data, columns=['delivery_id', 'customer_id', 'order_date', 'customer_pref_delivery_date']).astype({'delivery_id':'Int64', 'customer_id':'Int64', 'order_date':'datetime64[ns]', 'customer_pref_delivery_date':'datetime64[ns]'}))

window_spec = Window.partitionBy("customer_id").orderBy("order_date")
delivery = delivery.withColumn("rnk", sf.rank().over(window_spec))\
                    .withColumn("delivery_type", 
                               sf.when(sf.col("order_date")==sf.col("customer_pref_delivery_date"), "immediate")
                                 .otherwise("scheduled"))\
                    .filter(sf.col("rnk")==1)

delivery.groupBy().agg( 
    sf.round(100.0 * sf.sum(
                        sf.when(sf.col("delivery_type") == "immediate", 1).otherwise(0)
                    ) / sf.count("*"), 2)\
        .alias("immediate_percentage"))\
    .show()
# sql solutiond
# WITH cte AS (
# select delivery_id, customer_id, 
# RANK()OVER(PARTITION BY customer_id ORDER BY order_date) AS rnk, 
# CASE
# 	WHEN order_date = customer_pref_delivery_date THEN 'immediate'
# 	ELSE 'schedule'
# END as delivery_type
# FROM Delivery)

# SELECT ROUND(100.0*COUNT(*)FILTER(WHERE delivery_type='immediate')/COUNT(*),2) AS immediate_percentage 
# FROM cte
# WHERE rnk=1