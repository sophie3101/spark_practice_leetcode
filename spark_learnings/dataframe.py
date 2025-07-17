# https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName("LearnSpark").getOrCreate()

# createDataFrame
from datetime import datetime, date

"""DATAFRAME CREATION"""
# from list of rows
df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
# with explitcit schema
df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')
#from panas DataFrame
pandas_df = pd.DataFrame({
    'a': [1, 2, 3],
    'b': [2., 3., 4.],
    'c': ['string1', 'string2', 'string3'],
    'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
    'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
})
df = spark.createDataFrame(pandas_df)

#show dataframes,viewing data
df.printSchema()
df.show() # .show(n) fetches the first n rows (default n=20) from the executors to the driver.,t does not return the data.

# show row vertically
df.show(1, vertical=True)

#show summary of dataframe
df.select("a", "b", "c").describe().show()

df.collect() #scans the entire dataset, collects all partitions, and moves them to the driver.
# DataFrame.collect() collects the distributed data to the driver side as the local data in Python. Note that this can throw an out-of-memory error when the dataset is too large to fit in the driver side because it collects all the data from executors to the driver side.
# In order to avoid throwing an out-of-memory exception, use DataFrame.take() or DataFrame.tail().
df.take(1)

#to pands datafrrame
df.toPandas()

"""ACCESSING DATA"""
from pyspark.sql import Column
from pyspark.sql.functions import upper

type(df.c) == type(upper(df.c)) == type(df.c.isNull())

# return Column instance
df.a #Column<b'a'>
# select columns
df.select(df.c).show() # DataFrame.select() takes the Column instances that returns another DataFrame
# +-------+
# |      c|
# +-------+
# |string1|
# |string2|
# |string3|
# +-------+

# assign new COlumn instance
df.withColumn('upper_c', upper(df.c)).show()

#subset rows use DataFrame.filter().
df.filter(df.a == 1).show()
""" +---+---+-------+----------+-------------------+
|  a|  b|      c|         d|                  e|
+---+---+-------+----------+-------------------+
|  1|2.0|string1|2000-01-01|2000-01-01 12:00:00|
+---+---+-------+----------+-------------------+ """

"""APPLY A FUNCTION"""


"""WORKING WITH SQL"""
df.createOrReplaceTempView("tableA")
spark.sql("SELECT count(*) from tableA").show() 