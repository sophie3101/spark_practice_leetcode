

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf 
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("q1174").getOrCreate()

import pandas as pd
data = [[1, 2, '2016-03-01', 5], [1, 2, '2016-03-02', 6], [2, 3, '2017-06-25', 1], [3, 1, '2016-03-02', 0], [3, 4, '2018-07-03', 5]]
activity = spark.createDataFrame(pd.DataFrame(data, columns=['player_id', 'device_id', 'event_date', 'games_played']).astype({'player_id':'Int64', 'device_id':'Int64', 'event_date':'datetime64[ns]', 'games_played':'Int64'}))

cte = activity.groupBy("player_id").agg(sf.min("event_date").alias("first_login_date"))
join_df = activity.alias("a1").join(cte.alias("a2"),
               on=[
                   sf.col("a2.player_id")==sf.col("a1.player_id"),
                   sf.expr("a1.event_date=a2.first_login_date + INTERVAL 1 day")
                ], 
               how="right")
join_df.groupby().\
        agg(
            sf.round(
                sf.count("event_date")/sf.count("first_login_date"),2)\
                    .alias("fraction"))\
        .show()
# sql solutions
# with first_login_cte AS 
# (SELECT player_id, MIN(event_date) as first_login_date
# FROM activity 
# GROUP BY player_id
# ORDER BY player_id
# )
# SELECT ROUND(1.0 * COUNT(a1.event_date)/COUNT(a2.first_login_date), 2) as fraction
# FROM activity a1
# RIGHT JOIN first_login_cte a2 
# 	ON a2.player_id=a1.player_id  AND a1.event_date=a2.first_login_date + INTERVAL '1 day'