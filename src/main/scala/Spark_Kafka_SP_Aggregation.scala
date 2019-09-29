/**
  * * Use Spark-Kafka Integration - Structured Streaming
  *
  * - Run with JSON data (3 elements) created by CarparkSimulator.jar Brian's simulator
  * {"timestamp": "2019-08-24T00:00:01.538+12:00","nodeID": "57:21:61:6f:a8:28","payload": {"occupied": 1}}
  * - Using Kafka Producers to send messages from GWs to Kafka servers
  *
  * Note: change the path of "checkpoint" directory on your computer at this line
  * .config("spark.sql.streaming.checkpointLocation", "file:///home/pi/spark-applications/Kafka-checkpoint/checkpoint")
  */

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Spark_Kafka_SP_Aggregation {

  def main(args: Array[String]) {
    //******************************************************
    //=== Step 1: Create a Spark session, and run it on local mode =======

    // Local Mode
    val sparkMaster = "local[*]"
    val checkpointLocation = "file:///Users/brianchoi/checkpoint"
    val kafkaBrokers = "localhost:9092"
    val topicName = "sp-topic-1"

    // Production mode
    //    val sparkMaster = "spark://lpc01-master:7077"
    //    val checkpointLocation = "file:///home/pi/spark-applications/Kafka-checkpoint/checkpoint"
    //    val kafkaBrokers = "lpc01-kafka01:9092"
    //    val topicName = "sp-occupancy-1"

    val spark = SparkSession.builder()
      .appName("Spark_Kafka_SP_Aggregation")
      .master(sparkMaster)
      .config("spark.sql.streaming.checkpointLocation", checkpointLocation) //SANG: for RPi
      .getOrCreate()
    import spark.implicits._

    val rootLogger = Logger.getRootLogger().setLevel(Level.ERROR) //SANG: only display ERROR, not display WARN

    val zoneSchema = new StructType()
      .add("nodeID", StringType)
      .add("zoneID", IntegerType)

    val zoneDataFrame = spark
      .read
      .format("csv")
      .option("header", false)
      .schema(zoneSchema)
      .load("./src/main/resources/carpark_zones.csv")
      .as("zoneDataFrame")

    zoneDataFrame.show

    //******************************************************
    //=== Step 2: Read streaming messages from Kafka topics and select desired data for processing =======
    //=== Step 2.1: Read streaming messages from Kafka topics =======
    // Setup connection to Kafka
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", topicName) //subscribe Kafka topic name: sp-topic
      //.option("startingOffsets", "earliest")
      .option("startingOffsets", "latest")
      .load()

    //=== Step 2.1: Select the desired data elements from Kafka topics  =======
    val kafkaData = kafka
      //.withColumn("Key", $"key".cast(StringType))
      //.withColumn("Topic", $"topic".cast(StringType))
      //.withColumn("Offset", $"offset".cast(LongType))
      //.withColumn("Partition", $"partition".cast(IntegerType))
      .withColumn("Timestamp", $"timestamp".cast(TimestampType)) //SANG: this timestamp belongs to Kafka
      .withColumn("Value", $"value".cast(StringType))
      //.select("topic", "Key", "Value", "Partition", "Offset", "Timestamp")
      .select("Value", "Timestamp")

    val rawData = kafkaData.selectExpr("CAST(Value as STRING)")

    //******************************************************
    //=== Step 3: Define a schema and parse JSON messages =======
    //=== Step 3.1: Define a schema for JSON message read from Kafka topic =======
    // {"timestamp": "2019-08-24T00:00:01.538+12:00","nodeID": "57:21:61:6f:a8:28","payload": {"occupied": 1}}
    val dataStreamSchema = new StructType()
      .add("timestamp", StringType)
      .add("nodeID", StringType)
      .add("payload", (new StructType)
        .add("occupied", StringType)
      )

    //******************************************************
    //=== Step 3.2: Parse JSON messages =======
    val parkingData = rawData
      .select(from_json(col("Value"), dataStreamSchema).alias("parkingData")) //SANG: Value from Kafka topic
      .select("parkingData.*") //SANG: 2 lines are OK, but want to test the line below
      .select($"timestamp", $"nodeID", $"payload.occupied")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
      .withColumn("nodeID", $"nodeID".cast(StringType))
      .withColumn("occupied", $"occupied".cast(IntegerType))
      .join(zoneDataFrame, "nodeID").where($"nodeID" === $"zoneDataFrame.nodeID")

    // Change to kafka checkpoint
    val parkingDataPerNodePerWindow = parkingData
      .withWatermark("timestamp", "1 hour")
      .groupBy(
        $"nodeID",
        $"zoneID"
      )
      .agg(
        last("occupied") as "latest-occupancy",
        max("timestamp") as "latest-timestamp"
      )
      .orderBy("latest-timestamp")

    parkingDataPerNodePerWindow
      .selectExpr("CAST(nodeID AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("complete")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", "sp-output-topic")
      .start()

    val occupancyAggregation = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "sp-output-topic")
      //.option("startingOffsets", "earliest")
      .option("startingOffsets", "latest")
      .load()

    val dataStreamFromKafka = occupancyAggregation.selectExpr("CAST(Value as STRING)")
    dataStreamFromKafka.printSchema()

    val dataStreamFromKafkaStruct = new StructType()
      .add("nodeID", StringType)
      .add("zoneID", IntegerType)
      .add("latest-occupancy", StringType)
      .add("latest-timestamp", StringType)

    val latestOccupancyData = dataStreamFromKafka
      .select(from_json(col("Value"), dataStreamFromKafkaStruct).alias("latestOccupancyData"))
      .select("latestOccupancyData.*")
      .groupBy(
        "nodeID",
        "zoneID"
      )
      .agg(
        last("latest-timestamp"),
        last("latest-occupancy")
      )
      .withColumn("current-time", current_timestamp())

    latestOccupancyData
      .selectExpr("CAST(nodeID AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("complete")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", "sp-aggregate-topic")
      .start()

    val occupancyCountAggregation = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "sp-aggregate-topic") //subscribe Kafka topic name: sp-output-topic
      //.option("startingOffsets", "earliest")
      .option("startingOffsets", "latest")
      .load()

    val latestOccupancyDataStream = occupancyCountAggregation.selectExpr("CAST(Value as STRING)")
    latestOccupancyDataStream.printSchema()

    val latestOccupancyDataStruct = new StructType()
      .add("nodeID", StringType)
      .add("zoneID", IntegerType)
      .add("last(latest-timestamp, false)", StringType)
      .add("last(latest-occupancy, false)", StringType)
      .add("current-time", StringType)

    val latestOccupancyCountData = latestOccupancyDataStream
      .select(from_json(col("Value"), latestOccupancyDataStruct).alias("latestOccupancyCountData")) //SANG: Value from Kafka topic
      .select("latestOccupancyCountData.*")
      .groupBy("current-time", "zoneID")
      .agg(
        sum("last(latest-occupancy, false)")
      )
      .withColumnRenamed("sum(last(latest-occupancy, false))", "Occupancy-Count")
      .orderBy($"current-time".desc, $"zoneID")
      .select($"current-time", $"zoneID", $"Occupancy-Count")

    //    latestOccupancyCountData
    //      .selectExpr("to_json(struct(*)) AS value")
    //      .writeStream
    //      .format("kafka")
    //      .outputMode("complete")
    //      .option("kafka.bootstrap.servers", kafkaBrokers)
    //      .option("topic", "sp-current-occupancy")
    //      .start()
    //      .awaitTermination()

    latestOccupancyCountData
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }
}
