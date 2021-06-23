package PipeLine

import org.apache.spark.sql.SparkSession

object SparkConsumer {
  def main(args: Array[String]): Unit = {

    val topics = "test"
    val bootstrapServers = "localhost:9092"

    val spark = SparkSession.builder
      .appName("StructuredKafkaWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topics)
      .load()
      .selectExpr("CAST (key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    //Приложение отправляет какие-то свои логи в кафку
    //Мы читаем эти логи и выводим строку в котрой обнаружена ошибка и саму строку
    val source = lines.filter(x => x._2.contains("ERROR"))

    val query = source.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
