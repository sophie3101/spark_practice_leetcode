This repository contains my solutions for LeetCode SQL challenges. This is a good way to practice using Apache Spark (PySpark) and Spark SQL
example repo:
https://github.com/cM2908/leetcode-spark/blob/main/DataFrame%20Solutions/2051.%20The%20Category%20of%20Each%20Member%20in%20the%20Store%20(Medium).txt


How to run PySpark script

1. using spark-submit;
spark-submit your_script.py

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 2G \
  your_script.py


2. run interactively via pyspark shell


3. python <script>

This works only if the environment variables (SPARK_HOME, PYSPARK_PYTHON, etc.) are properly set.


4. in noteoobk
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("NotebookApp").getOrCreate()


4. docker
docker run -it --rm \
  -v /path/to/your/project:/app \
  -p 4040:4040 \
  apache/spark:latest /bin/bash
  
Inside the container:
apt-get update && apt-get install -y python3 python3-pip
pip3 install pyspark
spark-submit --master local[*] /app/spark_script.py
