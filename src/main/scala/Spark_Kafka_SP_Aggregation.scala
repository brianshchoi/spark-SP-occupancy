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
import org.apache.spark.sql.expressions.Window
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

    //    println("\n=== rawData schema ====")
    //    rawData.printSchema

    //******************************************************
    //=== Step 3: Define a schema and parse JSON messages =======
    //=== Step 3.1: Define a schema for JSON message read from Kafka topic =======
    // {"timestamp": "2019-08-24T00:00:01.538+12:00","nodeID": "57:21:61:6f:a8:28","payload": {"occupied": 1}}
    val schema = new StructType()
      .add("timestamp", StringType)
      .add("nodeID", StringType)
      .add("payload", (new StructType)
        .add("occupied", StringType)
      )

    //******************************************************
    //=== Step 3.2: Parse JSON messages =======
    val parkingData = rawData
      .select(from_json(col("Value"), schema).alias("parkingData")) //SANG: Value from Kafka topic
      .select("parkingData.*") //SANG: 2 lines are OK, but want to test the line below
      .select($"timestamp", $"nodeID", $"payload.occupied")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
      .withColumn("nodeID", $"nodeID".cast(StringType))
      .withColumn("occupied", $"occupied".cast(IntegerType)).as("parkingOccupied")

    // Change to kafka checkpoint
    val parkingDataPerNodePerWindow = parkingData
      .withWatermark("timestamp", "1 hour")
      .groupBy(
        $"nodeID"
      )
      .agg(
        last("occupied") as "latest-occupancy",
        max("timestamp") as "latest-timestamp"
      )
      .orderBy("latest-timestamp")

    //    parkingDataPerNodePerWindow
    //      .writeStream
    //      .outputMode("append")
    //      .format("kafka")
    //      .option("kafka.bootstrap.servers", kafkaBrokers)
    //      .option("topic", "sp-latest-topic")
    //      .option("checkpointLocation", latestJsonCheckpointLocation)
    //      .start()
    //      .awaitTermination()
//    parkingDataPerNodePerWindow
//      .writeStream
//      .outputMode("complete")
//      .format("console")
//      .option("truncate", false)
//      .start()
//      .awaitTermination()

    parkingDataPerNodePerWindow
      .selectExpr("CAST(nodeID AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("complete")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", "sp-output-topic")
      .start()


//    val spark2 = SparkSession.builder()
//      .appName("Spark_agg")
//      .master(sparkMaster)
//      .config("spark.sql.streaming.checkpointLocation", checkpointLocation2) //SANG: for RPi
//      .getOrCreate()

    val occupancyAggregation = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "sp-output-topic") //subscribe Kafka topic name: sp-output-topic
      //.option("startingOffsets", "earliest")
      .option("startingOffsets", "latest")
      .load()

    val rawData2 = occupancyAggregation.selectExpr("CAST(Value as STRING)")
    rawData2.printSchema()

    val schema2 = new StructType()
      .add("nodeID", StringType)
      .add("latest-occupancy", StringType)
      .add("latest-timestamp", StringType)

    val latestOccupancyData = rawData2
      .select(from_json(col("Value"), schema2).alias("latestOccupancyData")) //SANG: Value from Kafka topic
      .select("latestOccupancyData.*") //SANG: 2 lines are OK, but want to test the line below
      .groupBy("nodeID")
      .agg(
        last("latest-timestamp"),
        last("latest-occupancy")
      )
      .withColumn("current-time", current_timestamp())
    //      .select($"nodeID", $"latest-occupancy-string", $"current-time")
//      .withColumn("nodeID", $"nodeID")
//      .withColumn("latest-occup", $"nodeID".cast(StringType))
//      .withColumn("occupied", $"occupied")

//    latestOccupancyData
//      .writeStream
//      .outputMode("complete")
//      .format("console")
//      .option("truncate", false)
//      .start()
//      spark.streams.awaitAnyTermination()

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

    val rawData3 = occupancyCountAggregation.selectExpr("CAST(Value as STRING)")
    rawData3.printSchema()

    val schema3 = new StructType()
      .add("nodeID", StringType)
      .add("last(latest-timestamp, false)", StringType)
      .add("last(latest-occupancy, false)", StringType)
      .add("current-time", StringType)

    val latestOccupancyCountData = rawData3
      .select(from_json(col("Value"), schema3).alias("latestOccupancyCountData")) //SANG: Value from Kafka topic
      .select("latestOccupancyCountData.*") //SANG: 2 lines are OK, but want to test the line below
      .groupBy("current-time")
      .agg(
        sum("last(latest-occupancy, false)")
      )
      .withColumnRenamed("sum(last(latest-occupancy, false))", "Occupancy-Count")
      .orderBy($"current-time")
      .select($"current-time", $"Occupancy-Count" )

    latestOccupancyCountData
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("complete")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", "sp-current-occupancy-demo")
      .start()
      .awaitTermination()
  }
}
