# Spark Code Notes
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession


spark-shell --master yarn --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=10 --conf spark.dynamicAllocation.maxExecutors=500 --conf spark.shuffle.service.enabled=true --executor-cores 3 --executor-memory 18g --driver-memory 8g --conf spark.ui.port=0 --conf spark.sql.shuffle.partitions=3000 --conf spark.yarn.queue=queue1 --conf spark.sql.hive.convertMetastoreOrc=true  --files /app/gec/edw/udp/resources/sams-cdp-ada.json  --jars spark-bigquery-with-dependencies_2.11-0.23.2.jar --queue default


def readRawDataFrame(spark: SparkSession): DataFrame = {
           val srcTable = "sams-cdp-ada-prod.US_SAMS_CDP_STG.ABC"
     
          val srcDF = spark.read.format("bigquery").
          option("credentialsFile", "/app/gec/edw/udp/resources/sams-cdp-ada.json").
          option("parentProject", "sams-cdp-ada-prod").
          option("viewsEnabled", "true").
          option("viewMaterializationProject", "sams-cdp-ada-prod").
          option("viewMaterializationDataset", "US_SAMS_CDP_STG").
          option("query",s"SELECT * FROM $srcTable").load()
        srcDF
       }
 
val df = readRawDataFrame(spark) 
println(df.count())
