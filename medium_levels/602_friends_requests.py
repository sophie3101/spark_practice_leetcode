from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

spark = SparkSession.builder.appName("question602").getOrCreate()
import pandas as pd 
data = [[1, 2, '2016/06/03'], [1, 3, '2016/06/08'], [2, 3, '2016/06/08'], [3, 4, '2016/06/09']]
request_accepted = spark.createDataFrame(pd.DataFrame(data, columns=['requester_id', 'accepter_id', 'accept_date']).astype({'requester_id':'Int64', 'accepter_id':'Int64', 'accept_date':'datetime64[ns]'}))

df1=request_accepted.select(sf.col("requester_id").alias("id"))
df2=request_accepted.select(sf.col("accepter_id").alias("id"))
df=df1.unionAll(df2)
df_group = df.groupBy("id").agg(sf.count("*").alias("num"))
df_group.orderBy(sf.desc("num")).limit(1).show()
""" 
https://leetcode.com/problems/friend-requests-ii-who-has-the-most-friends/
sql solutions:
WITH cte AS (
	select requester_id
	FROM RequestAccepted r
	UNION  ALL
	select accepter_id as requester_id
	FROM RequestAccepted r
)
SELECT  
	requester_id as id,
	COUNT(*) as num
FROM cte
GROUP BY requester_id
ORDER BY COUNT(*) desc
LIMIT 1 """