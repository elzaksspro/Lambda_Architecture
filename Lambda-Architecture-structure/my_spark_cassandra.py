
# coding: utf-8

# In[1]:

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time


# In[2]:

from pyspark import SparkContext
sc = SparkContext()


# In[3]:

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In[4]:

data = sqlContext.read.csv("hdfs://localhost:9000/kafka_spark", inferSchema=True, header=True)


# In[5]:

def timestamp_conversion(timestamp):
        if timestamp is not None:
            return time.strftime('%Y-%m-%d', time.gmtime(timestamp/1000))
        else:
            return "null null"


# In[6]:

time_udf = udf(timestamp_conversion,StringType())


# In[7]:

inputDF = data.withColumn("timestamp_hour",time_udf(data["timestamp_hour"]))
inputDF.createOrReplaceTempView("activity")


# In[8]:

visitorsByProduct = sqlContext.sql("SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors FROM Activity GROUP BY product, timestamp_hour")
activityByProduct = sqlContext.sql("SELECT product, timestamp_hour, sum(case when action = 'purchase' then 1 else 0 end) as purchase_count, sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count, sum(case when action = 'page_view' then 1 else 0 end) as page_view_count from Activity group by product, timestamp_hour").cache()


# In[ ]:

visitorsByProduct.select("product", "timestamp_hour", "unique_visitors").write.    format("org.apache.spark.sql.cassandra").    options(table="batch_visitors_by_product", keyspace="lambda").    save(mode="append")

