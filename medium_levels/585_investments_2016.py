
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("question1934").getOrCreate() 

from pyspark.sql import functions as sf
from pyspark.sql.window import Window

import pandas as pd
data = [[1, 10, 5, 10, 10], [2, 20, 20, 20, 20], [3, 10, 30, 20, 20], [4, 10, 40, 40, 40]]
insurance = spark.createDataFrame(pd.DataFrame(data, columns=['pid', 'tiv_2015', 'tiv_2016', 'lat', 'lon']).astype({'pid':'Int64', 'tiv_2015':'Float64', 'tiv_2016':'Float64', 'lat':'Float64', 'lon':'Float64'}))

insurance.withColumn("loc", sf.concat_ws("_","lat", "lon"))\
    .withColumn("tiv_count", sf.count("*").over(Window.partitionBy("tiv_2015")))\
    .withColumn("loc_count", sf.count("*").over(Window.partitionBy("loc")))\
    .filter( (sf.col("loc_count")==1) & (sf.col("tiv_count")>1))\
    .agg(sf.round(sf.sum("tiv_2016"),2).alias("tiv_2016")).show()

""" -- SQL solution
WITH cte AS 
(SELECT pid,
count(*)OVER(PARTITION BY tiv_2015) as tiv_count,
count(*)OVER(PARTITION BY concat(lat,'-',lon)) as location_count
from insurance
)

SELECT ROUND(SUM(insurance.tiv_2016)::numeric, 2) AS tiv_2016
FROM insurance
JOIN cte 
	ON cte.pid=insurance.pid 
WHERE cte.tiv_count<>1 AND cte.location_count=1


--naive solution
WITH tiv_count_cte AS
	(select 
		tiv_2015, COUNT(*) as tiv_count
	FROM insurance
	GROUP BY tiv_2015
	HAVING COUNT(*) <>1
),location_cte AS (
	SELECT lat,lon, COUNT(*) as location_count
	FROM insurance
	GROUP BY lat,lon
	HAVING COUNT(*) <>1
)

SELECT ROUND(SUM(tiv_2016)::numeric, 2) AS tiv_2016
FROM insurance
WHERE NOT EXISTS (
	SELECT 1
	FROM location_cte
	WHERE location_cte.lat=insurance.lat AND location_cte.lon=insurance.lon
) AND EXISTS(
	SELECT 1
	FROM tiv_count_cte 
	WHERE tiv_count_cte.tiv_2015 = insurance.tiv_2015
) """