package eu.navispeed.rabbitmq
package table

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.unsafe.types.UTF8String

class RabbitMQPartitionReader(partition: InputPartition) extends PartitionReader[InternalRow] {

  private val value = RabbitMQInputPartition.from(partition).get.iterator()


  override def next(): Boolean = value.hasNext

  override def get(): InternalRow = InternalRow.apply(UTF8String.fromString(value.next))

  override def close(): Unit = {

  }
}
