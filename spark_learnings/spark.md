how to use:
- install spark and unzip using `tar -xzf...`
- the extracted folder is in /Users/u249637/Downloads/spark-4.0.0-bin-hadoop3

- to set env variable; add to ~/.bashrc or ~/.zshrc::
export SPARK_HOME=~/spark-4.0.0-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH

- to run spark shells, this to create SparkSession. 
    -scala shell: ./bin/spark-shell
    - python shell ./bin/pyspark
- submit spark job, this will submit the job to cluster manager. this deploy a PySpark application. 
the master URL will point to a specific cluster manager. The location of this cluster manager will be on a dedicated machine or set of machines within your cluster.
    `./bin/spark-submit --master local[2] your_script.py`

- running spark using docker:
docker pull apache/spark:latest

Run an interactive container first
docker run -it --rm \
  -v /path/to/your/project:/app \
  -p 4040:4040 \
  apache/spark:latest /bin/bash
  Inside the container:
apt-get update && apt-get install -y python3 python3-pip
pip3 install pyspark
spark-submit --master local[*] /app/spark_script.py

OR 

docker run -it --rm \
  -p 4040:4040 \
  apache/spark:latest ../bin/pyspark



-- running spark using jupyter notebook:
    docker run -it --rm -p 8888:8888 jupyter/pyspark-notebook
if u want to save the script
    docker run -it --rm -p 8888:8888 -v $PWD:/home/jovyan/work jupyter/pyspark-notebook

RDD (Residlient Distributed Datasets)

1. Spark is written in Scala. two  options we recommend for getting started with Spark: downloading and installing
Apache Spark on your laptop, or running a web-based version in Databricks Community Edition, a
free cloud environment 
- Spark Application has driver process and executor processes. driver process runs main() function, sits on node in a cluster, and responsible for: 
    - maining information of sprak applicatin
    - respond to user's program or input
    - analyzing, distributingand scheduling work across executors
- executors: reponsible for acually carrying out the work driver assigns them
+---------------------+
|     Driver Program  |
|  - SparkSession     |
|  - Task Scheduler   |
+---------------------+
           |
   Sends tasks to
           v
+---------------------+     +---------------------+
|   Executor 1        | ... |   Executor N        |
| - Runs tasks        |     | - Runs tasks        |
| - Stores data cache |     | - Stores data cache |
+---------------------+     +---------------------+

- cluster manager: control physical machie and allocate resources to Spark appliation. it can be: Spark standalone cluster manager, Yarn or Mesos

Here are the key points to understand about Spark Applications at this point:
    - Spark employs a cluster manager that keeps track of the resources available.
    - The driver process is responsible for executing the driver program’s commands across the
executors to complete a given task.
![](images/spark_architecture.png)
![](images/spark_api.png)

-when you start Spark in this interactive mode, you implicitly create a SparkSession that manages the
Spark Application. When you start it through a standalone application, you must create the
SparkSession object yourself in your application code.

## Transformation
two types of transforamtions: narrow dependencies (1-> 1, in-memory) and wide dependencies (wrte resutls to disk)

- lazy evaluation: wait till last moment to execute computation isntructions
- to trigger computation, run an action
Transformations: Create a new RDD/DataFrame (e.g., map, filter). Lazy-evaluated. create a logical execution plan (a Directed Acyclic Graph, or DAG) but don't execute immediately. They build a lineage of operations without processing data.
Actions: Trigger computation and return results (e.g., collect, count).

- monitor progress of a job through Spark web UI. Spark UI avaiable on port 4040 of driver node 
- ntoe; by default Spark outputs 200 shuffle partitions. to specify a number of partition: spark.conf.set("spark.sql.shuffle.partitions"
,"5")
http://localhost:4040/jobs/


# resources
https://spark.apache.org/docs/latest/