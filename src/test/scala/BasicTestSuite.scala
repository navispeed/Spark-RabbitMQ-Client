package eu.navispeed.rabbitmq

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuiteLike

class BasicTestSuite extends AnyFunSuiteLike {

  val sparkSession: SparkSession = SparkSession.builder().master("local[8]").appName("it").getOrCreate()



  test("should read message from rabbitmq") {
    def myFunc( askDF:DataFrame, batchID:Long ) : Unit = {
      askDF.persist()
      askDF.show()
      askDF.unpersist()
    }

    sparkSession.readStream.format(RabbitMQSource.name).load().writeStream.foreachBatch(myFunc _).start().awaitTermination()
  }
}
