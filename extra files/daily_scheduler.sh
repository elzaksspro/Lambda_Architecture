DATE=`date +%y-%m-%d`

basePath="hdfs://localhost:9000/flume_data/sensor-data/"

inputPath=$basePath$DATE
outputPath=$basePath"cleaned/"$DATE

pig -f dataCleansing.pig -param inputPath=$inputPath -param outputPath=$outputPath
rValue=$?
if [ $rValue -ne 0 ]; then
    echo "Error"
    exit 1
fi

mv pif*.log ./piglogs
#if [ $inputPath != $basePath ] then
#    hadoop fs -rm -skipTrash $inputPath
#fi
