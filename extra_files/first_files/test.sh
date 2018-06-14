DATE=`date +%y-%m-%d`

basePath="hdfs://localhost:9000/flume_data/sensor-data/"

inputPath=$basePath$DATE
outputPath=$basePath"cleaned/"$DATE

echo pig -f dataCleansing.pig -param inputPath=$inputPath outputPath=$outputPath
