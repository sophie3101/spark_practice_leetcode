# https://leetcode.com/problems/department-highest-salary/description/
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf 
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("q1174").getOrCreate()

import pandas as pd
data = [[1, 'Joe', 70000, 1], [2, 'Jim', 90000, 1], [3, 'Henry', 80000, 2], [4, 'Sam', 60000, 2], [5, 'Max', 90000, 1]]
employee = spark.createDataFrame(pd.DataFrame(data, columns=['id', 'name', 'salary', 'departmentId']).astype({'id':'Int64', 'name':'object', 'salary':'Int64', 'departmentId':'Int64'}))
data = [[1, 'IT'], [2, 'Sales']]
department = spark.createDataFrame(pd.DataFrame(data, columns=['id', 'name']).astype({'id':'Int64', 'name':'object'}))

window_spec = Window.partitionBy("departmentId").orderBy(sf.col("salary").desc())
employee=employee.withColumn("salary_rank", sf.rank().over(window_spec))\
                    .selectExpr("name AS Employee", "salary", "departmentID", "salary_rank")
joined_df = employee.join(department, employee.departmentID==department.id, 'inner')\
    .filter(sf.col("salary_rank")==1)\
    .selectExpr("Employee", "salary", "name AS department")
joined_df.show()
    

#sql solution
""" WITH cte AS
(
select name, salary, departmentid, 
RANK()OVER(PARTITION BY departmentid ORDER BY salary DESC) as salary_rnk
FROM employee
)

SELECT d.name as Department, cte.name AS Employee, cte.salary AS Salary
FROM department d
JOIN cte 
	ON cte.departmentid=d.id
WHERE salary_rnk=1 """