package eu.navispeed.rabbitmq

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Channel, ConnectionFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.scalatest.funsuite.AnyFunSuiteLike

case class Model(id: Long)

class BasicTestSuite extends AnyFunSuiteLike {

  private val factory = new ConnectionFactory()

  factory.setHost("localhost")
  factory.setUsername("guest")
  factory.setPassword("guest")
  factory.setVirtualHost("/")

  private val channel: Channel = factory.newConnection().createChannel()

  val sparkSession: SparkSession = SparkSession.builder().master("local[4]").appName("it").getOrCreate()

  test("should read message from rabbitmq") {
    var res: Array[Model] = Array()

    def myFunc(askDF: Dataset[Model], batchId: Long): Unit = {
      res = askDF.collect()
    }

    channel.basicPublish("test", "#", new BasicProperties(), "{\"id\": 1}".getBytes)
    channel.basicPublish("test", "#", new BasicProperties(), "{\"id\": 2}".getBytes)
    channel.basicPublish("test", "#", new BasicProperties(), "{\"id\": 3}".getBytes)
    channel.basicPublish("test", "#", new BasicProperties(), "{\"id\": 4}".getBytes)
    channel.basicPublish("test", "#", new BasicProperties(), "{\"id\": 5}".getBytes)

    implicit val encoder: Encoder[Model] = Encoders.product[Model]
    sparkSession.readStream
      .format(RabbitMQSource.name)
      .options(Configuration(queueName = "test"))
      .load()
      .withColumn("value", from_json(col("json"), encoder.schema))
      .select("value.*")
      .as[Model]
      .writeStream
      .foreachBatch(myFunc _)
      .trigger(Trigger.Once())
      .start()
      .awaitTermination()

    assert(res.length > 0)
  }
}
