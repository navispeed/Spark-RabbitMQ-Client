package eu.navispeed.rabbitmq

import eu.navispeed.rabbitmq.client.BasicRabbitMQClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuiteLike

case class Model(id: Long)

class BasicTestSuite extends AnyFunSuiteLike {


  val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("it").getOrCreate()

  test("should read message from rabbitmq") {
    var res: Array[Model] = Array()

    def myFunc(askDF: Dataset[Model], batchId: Long): Unit = {
      res = askDF.collect()
    }

    val rmqClient = new BasicRabbitMQClient(new Configuration(queueName = ""))

    rmqClient.send("test", "{\"id\": 1}")
    rmqClient.send("test", "{\"id\": 2}")
    rmqClient.send("test", "{\"id\": 3}")
    rmqClient.send("test", "{\"id\": 4}")
    rmqClient.send("test", "{\"id\": 5}")

    implicit val encoder: Encoder[Model] = Encoders.product[Model]
    sparkSession.readStream
      .format(RabbitMQSource.name)
      .load()
      .withColumn("value", from_json(col("json"), encoder.schema))
      .select("value.*")
      .as[Model]
      .writeStream
      .foreachBatch(myFunc _)
      .trigger(Trigger.Once())
      .start()
      .awaitTermination()

    assert(res.length == 5)
  }
}
