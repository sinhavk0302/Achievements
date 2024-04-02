cat ryan_sales_affinity_weekly.py

import sys
import os
import pyspark.sql.functions as f
from pyspark.sql.functions import when, sum, col, lit
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark

import calendar
from datetime import date, datetime, timedelta
from pyspark.conf import SparkConf


process="Sales affinitely weekly raw"
conf = SparkConf().setAppName("Pyspark : {}".format(process)).set("spark.sql.files.maxPartitionBytes","10737418240").set("hive.exec.dynamic.partition.mode","nonstrict").set("spark.sql.sources.partitionOverwriteMode","dynamic").set("spark.sql.sources.partitionOverwriteMode","dynamic").set("spark.sql.parquet.writeLegacyFormat", "true").set("spark.sql.autoBroadcastJoinThreshold","50000")

# Create a SparkSession object
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

# Retrieve the SparkConf object from the SparkContext
conf = spark.sparkContext.getConf()

# Print the configuration settings
print("spark.app.name = ", conf.get("spark.app.name"))
print("spark.master = ", conf.get("spark.master"))
print("spark.deploy.mode = ", conf.get("spark.deploy.mode"))
print("spark.driver.memory = ", conf.get("spark.driver.memory"))
print("spark.driver.cores = ", conf.get("spark.driver.cores"))
print("spark.executor.memory = ", conf.get("spark.executor.memory"))
print("spark.executor.cores = ", conf.get("spark.executor.cores"))
print("spark.sql.shuffle.partitions=",conf.get("spark.sql.shuffle.partitions"))

salesExtract="""
SELECT
    a.transaction_id,
    a.guest_cluster_id,
    a.basket_id,
    a.sale_d,
    a.tcin,
    sum(a.extended_net_a) as extended_net_a
FROM
    prd_gdp_fnd.guest_sales_all a
WHERE
  a.sale_d >= add_months(date_sub(current_date,cast(mod(from_unixtime(unix_timestamp(current_date),'u'),7) as int)), -26)
  AND a.sale_d < date_sub(current_date,cast(mod(from_unixtime(unix_timestamp(current_date),'u'),7) as int))
GROUP BY
    a.guest_cluster_id,
    a.transaction_id,
    a.basket_id,
    a.sale_d,
    a.tcin
HAVING sum(a.sale_q) >0
"""
# Read data into spark dataframe for spark to apply guest_sales_all view in spark memeory by translating above hive SQL into hdfs based file reads and join three tables from view in spark memory
salesDF=spark.sql(salesExtract)

# Write spark dataframe into 200 parquet file for breaking the spark DAG chain. This will help in next step, when files will be read in parallel and joined with other datasets in 200 parallel threads. If write is poor performing, we can avoid it
salesDF.write.format("parquet").mode("overwrite").save("/user/Z062155/hive/ryan_guest_page_sales_weekly_input")

#Read data from parquet file into temp table
salesDFRead = spark.read.load("/user/Z062155/hive/ryan_guest_page_sales_weekly_input")

salesDFRead.createOrReplaceTempView("guest_sales_data")

sqlmapTcin="""
SELECT
        a.transaction_id,
        a.guest_cluster_id,
        a.basket_id,
        a.sale_d,
        date_sub(a.sale_d,cast(mod(from_unixtime(unix_timestamp(a.sale_d),'u'),7) as int)) as sale_wk,
        a.extended_net_a,

        a.tcin,
        c.page_level_n,
        c.page_n,
        c.page_id

    FROM
        guest_sales_data a
        LEFT JOIN
        prd_ai_cap_fnd.tcin_to_page_mapping c on
            a.tcin = c.tcin
"""
salesWithmapTCINDF = spark.sql(sqlmapTcin)
salesWithmapTCINDF.createOrReplaceTempView("transaction_data")

finalSQL="""
select
    guest_cluster_id,
    sale_wk,
    page_id,
    count(distinct transaction_id) as net_trips,
    sum(extended_net_a) as net_amount
from
    transaction_data
group by
    guest_cluster_id,
    sale_wk,
    page_id
CLUSTER BY
    guest_cluster_id,
    sale_wk
"""
finalData=spark.sql(finalSQL)

finalData.write.mode("overwrite").format("parquet").save("/user/Z062155/hive/guest_page_sales_affinity_weekly_raw_inputs")
