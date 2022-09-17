package eu.navispeed.rabbitmq
package table

import org.apache.spark.sql.connector.read.InputPartition

private [table] class RabbitMQInputPartition(messages: List[String]) extends InputPartition {

  def iterator(): Iterator[String] = messages.iterator

}

private [table] object RabbitMQInputPartition {
  def from(inputPartition: InputPartition): Option[RabbitMQInputPartition] = inputPartition match {
    case rabbitMQInputPartition: RabbitMQInputPartition => Option.apply(rabbitMQInputPartition)
    case _ => Option.empty
  }
}