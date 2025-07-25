# RDD
Spark revolves around the concept of a resilient distributed dataset (RDD), which is a fault-tolerant collection of elements that can be operated on in parallel. There are two ways to create RDDs: parallelizing an existing collection in your driver program, or referencing a dataset in an external storage system,

RDDs support two types of operations: transformations, which create a new dataset from an existing one, and actions, which return a value to the driver program after running a computation on the dataset.

All transformations in Spark are lazy, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program

1. Transformations (lazy)
Return a new RDD.

- map()
- filter()
- flatMap()
- union()
- distinct()
- groupByKey()
- reduceByKey()
#### Narrow Transformation
A transformation where each output partition depends on only one input partition.
This means data is not shuffled across the network.Examples: map(), filter(), union(), mapPartitions()

```
val rdd1 = sc.parallelize(1 to 10, 2)  // 2 partitions
val rdd2 = rdd1.map(x => x * 2)        // map is narrow, no shuffle
```

#### Wide Transformation
A transformation where each output partition depends on multiple input partitions.
This requires shuffling data across the network between executors.
Examples: reduceByKey(), groupByKey(), join(), distinct(), sortByKey()

2. Actions (trigger execution) Return a result.

- collect()
- count()
- take(n)
- first()
- reduce()
- saveAsTextFile()

## Dataframe
DataFrames provide a higher-level abstraction on top of RDDs:\
They add schema information (column names and types).\
They provide optimized query execution using Spark’s Catalyst optimizer.\
They allow SQL-like operations and more expressive APIs.\
Under the hood, a DataFrame is implemented using RDDs of Row objects. But as a user, you interact with DataFrames, which hide the complexity of RDDs and give you a much richer interface.

## shuffling

Shuffling is an important step in a Spark job whenever data is rearranged between partitions