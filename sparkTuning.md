# Spark Application Tuning
## Resolving Executor OOM (Out of Memory) issues
Executor OOM issue can occur when the allocated memory of an executor is exceeded.
Use Executor OOM issue to optimize Spark applications and improve performance
#### How:
Monitor applications executor memory usage and observe which specific stage(s) or tasks is causing OOM, once identified, fix/tune code on those specific section of program.
Some of the techniques include:
Optimizing data serialization format.
Reducing un-necessary shuffling of data.
Efficient partitioning of data.
Utilizing spark caching mechanism appropriately.
Pay attention to JVM settings and memory configurations for optimal memory utilization.
## Salting technique
Minimizing shuffling in spark is crucial to spark application performance and reducing data movement across nodes. This can be done with proper partitioning and leveraging operations that can be performed locally within a partition.
Salting is a technique used in spark data processing and encryption to enhance security and distribute data evenly across partitions.
Salting can be used while dealing with the skewed data and distributions. Skewness happens when certain keys or values have significantly higher frequency than others, resulting in imbalanced partitions and poorer data processing.
 
## Data Locality
It refers to the principle of bringing computation closer to the data spark operates on. It plays a crucial role in minimizing data movement across network:
#### How:
Analyze data and processing tasks involved
Examine partitioning and distribution of data: Fine tune partition by ensuring data is evenly distributed across cluster, factors include: data size, processing requirements and available resources
Identify opportunities to localize the data locality: Use of spark data placement options: colocation. Colocated related datasets on same nodes to maximize data locality. This reduces network overhead
Optimized task scheduling and resource allocation: Ensure tasks are allocated to nodes where data is already present.

## Garbage Collection (GC) - GC Overhead
GC tuning in spark is critical for optimizing memory management in Spark. Can greatly impact the STABILITY of spark application.
#### How:
Configure JVM's GC parameters (heap size, GC algorithm ) to efficiently manage memory usage and minimize pauses caused by GC collection activities and reduced GC overhead.
Identify if the spark application is memory intensive application that frequently experiences long GC pauses and optimize it.
Understand the memory utilization and workload of spark applications using tools (Spark monitoring framework, Java profilers)

## Predicate Push-down and partition purning (Most of us are knowingly, unknowingly designing it)
Predicate push-down involves pushing the filtering conditions or predicates closer to the data source, such as Dataframe or SQL query. This minimizes the amount of data that needs to be processed
Partition Pruning : it is technique using which sparks eliminates the filtered partitions completely based on criteria
Minimizing shuffling : appropriate partitioning strategy so that same data is colocated. Leverage operations like reduceByKey or aggregateByKey instead of groupByKey. These operations perform aggregations at partition level before combining the results.
 
## Adaptive Query Engine: 

[Spark 2.x to spark 3.0 — Adaptive Query Execution — Part1](https://medium.com/data-engineering-for-all/spark-2-x-to-spark-3-0-adaptive-query-execution-part1-182e61e6cfcb) 
Adaptive Query Engine in Spark is a feature that dynamically adjusts query execution plans based on runtime data stats. It allows spark to adopt and optimize its execution strategy as it gathers insights about data during runtime. It adjusts join strategies, broadcasts threshold, and partitioning schemes based on data size and distribution
In case we have a large dataset with varying partitions sizes, AQE with run time stats can identify partitions with skewed data distribution and then dynamically repartition the data to ensure a more balanced workload across executors.
It also provides runtime filter optimization: It skips reading unwanted data based on runtime stats resulting in faster processing of data.

## DAG: Breaking the spark DAG to optimize performance:
It involves breaking long chain of transformation into smaller stages to enhance parallelism and optimize resource utilization. This can lead to significant performance improvement in Spark applications:
Breaking spark DAG into stages allows for better pipelining and parallel processing
### How:
One approach is by reading and writing into intermediate results as Parquet files. It introduces a natural stage boundary in spark DAG.
Parquet files provide columnar storage and compression, which can improve I/O efficiency and reduce disk space usage.
By persisting intermediate results as Parquet files, subsequent stages can read data directly from these files, avoiding unnecessary recomputation.
Partitioning the data in Parquet files allows spark to perform more targeted and efficient operations.
By leveraging Parquet files strategically can significantly improve spark application performance, especially while dealing with large volume datasets.

## Cost Based Optimizer (CBO):
By analyzing the statistics and cost of various execution plans, optimizer can intelligently chose the most efficient plan to execute a given query:
Ex: Spark application that involves joining of two large datasets. Instead of relying on rule based optimizer, enabling cost based optimizer in spark. CBO users statistical information about the data, such as column cardinality and distribution, to estimate the cost of different execution plans.
CBO considers various factors like data size, network shuffle, and available resources t estimate execution time and resource consumption of different execution plan. It assigns cost to each plan and selects the one with lowest cost as optimal plan
Enabling CBO allows spark to dynamically adapt to different data and cluster conditions. It can optimize join strategies, chose efficiently join algorithms like broadcast or shuffle and even reorder operations within a query plan.
Result: reduce network shuffling, overall better resource utilization and improved performance
Set spark.sql.cbo.enabled to true, additionally set the related properties like statistics collection and refresh interval for accurate cost estimation.

## Data Compressions:
Utilizing compression in spark is a game changer for optimizing data storage and improving spark performance..
Compression in spark helps reduce the size of data files stored in distributed file systems. Compressed files can be read and written more efficiently resulting in reduced I/O operations
How do decide on a compression algorithm :
Choice of compression algorithm depends upon various factors such as data type, desired compression ratio and performance requirements.
Snappy is good choice for data that requires fast compression and decompression with a moderate compression ratio.
Gzip provides higher compression ratio but might require more CPU resources
