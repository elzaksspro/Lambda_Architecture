---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
																		STEPS FOR PREDICTIVE MAINTENANCE FOR BATCH MODE PART
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------

1. Start Zookeeper Server

zkServer.sh start /home/shravan/zookeeper/config/zookeeper.properties


2. Start Kafka Server

kafka-server-start.sh /home/shravan/kafka/config/server.properties


3. Create a kafka topic

kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic data_pipeline


4. You can check that your topic is created ot not using the following command

kafka-topics.sh --list --zookeeper localhost:2181

kafka-console-consumer.sh --zookeeper localhost:2181 --topic data_pipeline --from-beginning

5. Start a Kafka Producer to produce live data for that

kafka-console-producer.sh --broker-list localhost:9092 --topic sensor


6. Start Hadoop and its services

7. Configure flume agent for HDFS to get data from Kafka

Configuration file: kafka.config

#	Flume	config	to	listen	to	Kakfa	topic	and	write	to	HDFS.
flume1.sources	=	kafka-source-1
flume1.channels	=	hdfs-channel-1
flume1.sinks	=	hdfs-sink-1
#	For	each	source,	channel,	and	sink,	set
#	standard	properties.
flume1.sources.kafka-source-1.type	=	org.apache.flume.source.kafka.KafkaSource
flume1.sources.kafka-source-1.zookeeperConnect	=	localhost:2181
flume1.sources.kafka-source-1.topic	=	sensor
flume1.sources.kafka-source-1.batchSize	=	100
flume1.sources.kafka-source-1.channels	=	hdfs-channel-1
flume1.channels.hdfs-channel-1.type	=	memory
flume1.sinks.hdfs-sink-1.channel	=	hdfs-channel-1
flume1.sinks.hdfs-sink-1.type	=	hdfs
flume1.sinks.hdfs-sink-1.hdfs.writeFormat	=	Text
flume1.sinks.hdfs-sink-1.hdfs.fileType	=	DataStream
flume1.sinks.hdfs-sink-1.hdfs.filePrefix	=	sensor
flume1.sinks.hdfs-sink-1.hdfs.useLocalTimeStamp	=	true
flume1.sinks.hdfs-sink-1.hdfs.path	=	/flume_data/%{topic}-data/%y-%m-%d
flume1.sinks.hdfs-sink-1.hdfs.rollCount=100
flume1.sinks.hdfs-sink-1.hdfs.rollSize=0
#	Other	properties	are	specific	to	each	type	of
#	source,	channel,	or	sink.	In	this	case,	we
#	specify	the	capacity	of	the	memory	channel.
flume1.channels.hdfs-channel-1.capacity	=	10000


8. Create the designated directory in HDFS

9. Start the flume agent services

flume-ng agent --conf conf --conf-file /home/shravan/Flume/kafka.config --name flume1 -Dflume.root.logger=INFO,console


10. Open the HDFS web interface and check that file or data is going into the HDFS or not.


<--------------------------------------------------------------------------------------------------------------------------->

SUCCESSFULL............

<--------------------------------------------------------------------------------------------------------------------------->

$SPARK_HOME/bin/spark-shell --packages datastax:spark-cassandra-connector:2.0.0-M2-s_2.11 

'spark-submit','--packages', 'mysql:mysql-connector-java:5.1.39', '--master', 'local[4]', 'label_predicted_spark_job.py'

spark-submit --packages datastax:spark-cassandra-connector-2.0.0-M2-s_2.11 --master local[4] my_spark_cassandra.py


original:-
'INSERT INTO batch_visitors_by_product (product, timestamp_hour, unique_visitors) VALUES ('{0}','{1}',{2})" \
                .format(prod_name, row['timestamp_hour'], row['unique_visitors'])


'INSERT INTO batch_visitors_by_product (product, timestamp_hour, unique_visitors) VALUES ('{}','{}','{}')'.format(prod_name, row['timestamp_hour'], row['unique_visitors'])

q = 'INSERT INTO batch_visitors_by_product (product, timestamp_hour, unique_visitors) VALUES ({},{},{})'.format("a", "b", "c")



CREATE TABLE lambda.stream_visitors_by_product_text (
    product text,
    timestamp_hour text,
    unique_visitors bigint,
    PRIMARY KEY (product, timestamp_hour)
) WITH CLUSTERING ORDER BY (timestamp_hour DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';