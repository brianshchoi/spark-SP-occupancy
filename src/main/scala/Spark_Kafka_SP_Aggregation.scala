/** 
  ** Use Spark-Kafka Integration - Structured Streaming
  *
  * - Run with JSON data (3 elements) created by CarparkSimulator.jar Brian's simulator
  *   {"timestamp": "2019-08-24T00:00:01.538+12:00","nodeID": "57:21:61:6f:a8:28","payload": {"occupied": 1}}
  * - Using Kafka Producers to send messages from GWs to Kafka servers
  *
  *  Note: change the path of "checkpoint" directory on your computer at this line
  *  .config("spark.sql.streaming.checkpointLocation", "file:///home/pi/spark-applications/Kafka-checkpoint/checkpoint")
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.log4j._

object Spark_Kafka_SP_Aggregation {

  def main(args: Array[String]) {
    //******************************************************
    //=== Step 1: Create a Spark session, and run it on local mode =======
    val spark = SparkSession.builder()
      .appName("Spark_Kafka_SP_Aggregation")

      // Desktop application
      .master("local[2]")

      // Cluster application
//      .master("spark://lpc01-master:7077")
      .config("spark.sql.streaming.checkpointLocation", "file:///Users/brianchoi/checkpoint") //SANG: for RPi
      .getOrCreate()
    import spark.implicits._

    val rootLogger = Logger.getRootLogger().setLevel(Level.ERROR)   //SANG: only display ERROR, not display WARN

    //******************************************************
    //=== Step 2: Read streaming messages from Kafka topics and select desired data for processing =======
    //=== Step 2.1: Read streaming messages from Kafka topics =======
    val kafkaBrokers ="localhost:9092"  // Kafka server

    // Setup connection to Kafka
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "sp-topic")  //subscribe Kafka topic name: sp-topic
      //.option("startingOffsets", "earliest")
      .option("startingOffsets", "latest")
      .load()

    //=== Step 2.1: Select the desired data elements from Kafka topics  =======
    val kafkaData = kafka
      //.withColumn("Key", $"key".cast(StringType))
      //.withColumn("Topic", $"topic".cast(StringType))
      //.withColumn("Offset", $"offset".cast(LongType))
      //.withColumn("Partition", $"partition".cast(IntegerType))
      .withColumn("Timestamp", $"timestamp".cast(TimestampType))  //SANG: this timestamp belongs to Kafka
      .withColumn("Value", $"value".cast(StringType))
      //.select("topic", "Key", "Value", "Partition", "Offset", "Timestamp")
      .select("Value", "Timestamp")

    val rawData = kafkaData.selectExpr("CAST(Value as STRING)")

    println("\n=== rawData schema ====")
    rawData.printSchema

    //******************************************************
    //=== Step 3: Define a schema and parse JSON messages =======
    //=== Step 3.1: Define a schema for JSON message read from Kafka topic =======
    // {"timestamp": "2019-08-24T00:00:01.538+12:00","nodeID": "57:21:61:6f:a8:28","payload": {"occupied": 1}}
    val schema = (new StructType)
      .add("timestamp", StringType)
      .add("nodeID", StringType)
      .add("payload", (new StructType)
        .add("occupied", StringType)
      )

    //******************************************************
    //=== Step 3.2: Parse JSON messages =======
    val parkingData = rawData
      .select(from_json(col("Value"), schema).alias("parkingData")) //SANG: Value from Kafka topic
      .select("parkingData.*")  //SANG: 2 lines are OK, but want to test the line below
      .select($"timestamp", $"nodeID", $"payload.occupied")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
      .withColumn("nodeID", $"nodeID".cast(StringType))
      .withColumn("occupied", $"occupied".cast(IntegerType))

    println("\n=== parkingData schema ====")
    parkingData.printSchema

    //******************************************************
    //=== Step 4: Process data =======
    val parkingData_agg = parkingData
      .withWatermark("timestamp", "5 minutes")
      .groupBy(window($"timestamp", "5 minutes", "5 minutes"))
      .agg(sum("occupied") as "Current-occupancy")

      //.withColumn("current-time", $"window.end")
      .withColumn("current-time", $"window.start")
      .select( "current-time", "Current-occupancy")
      .orderBy("current-time")

    //******************************************************
    //=== Step 5: Output streaming result =======
    parkingData_agg
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()

  }
}
