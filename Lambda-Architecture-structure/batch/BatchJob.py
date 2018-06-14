from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from pyspark.sql import SQLContext

spark = SparkSession.builder.master("local").appName("Lambda_Batch_Job").config('spark.cassandra.connection.host', 'localhost').getOrCreate()

sqlContext = SQLContext(spark)

inputDF = spark.read.format("csv").option("header","true").option("delimiter","\t").option("inferSchema","true").load("../../input_data/data.tsv")

def timestamp_conversion(timestamp):
        if timestamp is not None:
            return time.strftime('%Y-%m-%d', time.gmtime(timestamp/1000))
        else:
            return "null null"

time_udf = udf(timestamp_conversion,StringType())

inputDF = inputDF.withColumn("timestamp_hour",time_udf(inputDF["timestamp_hour"]))
inputDF.createOrReplaceTempView("Activity")

visitorsByProduct = spark.sql("SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors FROM Activity GROUP BY product, timestamp_hour")

activityByProduct = spark.sql("SELECT product, timestamp_hour, sum(case when action = 'purchase' then 1 else 0 end) as purchase_count, sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count, sum(case when action = 'page_view' then 1 else 0 end) as page_view_count from Activity group by product, timestamp_hour").cache()

visitorsByProduct.write.format("org.apache.spark.sql.cassandra").options(table="batch_visitors_by_product", keyspace = "lambda").save()
