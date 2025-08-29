from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("q1204").getOrCreate()

import pandas as pd
data = [[1, 'Abbot'], [2, 'Doris'], [3, 'Emerson'], [4, 'Green'], [5, 'Jeames']]
seat = spark.createDataFrame(pd.DataFrame(data, columns=['id', 'student']).astype({'id':'Int64', 'student':'object'}))

window_spec = Window.orderBy("id")
max_id=seat.count()
seat = seat.withColumn("next_student", sf.lead("student").over(window_spec))\
            .withColumn("prev_student", sf.lag("student").over(window_spec))
max_id=seat.count()
seat.withColumn("arranged_student", \
    sf.when((sf.col("id") % 2 == 1) & (sf.col("id") == max_id), sf.col("student"))\
    .when(sf.col("id") % 2 == 0, sf.col("prev_student"))\
    .otherwise(sf.col("next_student"))
).select("id", sf.col("arranged_student").alias("student"))\
.show()

""" 
sql solution
--solution 1
 WITH cte AS 
(select id , student as current_student,
LAG(student)OVER(ORDER BY id) as previous_student,
LEAD(student)OVER(ORDER BY id) as next_student,
LAST_VALUE(student)OVER() as last_student
 FROM Seat
)
SELECT id,
	CASE 
		WHEN id%2=1 AND current_student!=last_student THEN next_student
		WHEN id%2=0 THEN previous_student
		WHEN id%2=1 AND current_student=last_student THEN last_student
	END AS student
 FROM cte 

 --solution 2
 WITH cte AS 
(SELECT id,
	CASE
		WHEN id %2=0 THEN id-1
		WHEN id%2=1 AND id!=(select max(id) FROM Seat) THEN id+1
		ELSE id
	END as new_id
FROM Seat
)

SELECT cte.id, Seat.student
FROM cte
JOIN Seat ON Seat.id=cte.new_id
ORDER BY cte.id
 """