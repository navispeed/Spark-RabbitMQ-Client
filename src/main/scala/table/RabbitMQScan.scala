package eu.navispeed.rabbitmq
package table

import client.BasicRabbitMQClient

import net.liftweb.json._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.types.StructType

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters
import scala.util.control.Breaks._

private[table] class RabbitMQScan(schema: StructType, configuration: Configuration) extends Scan {

  private val currentOffset = new AtomicLong()
  private val buffer = new ConcurrentLinkedDeque[(Long, String, () => Unit)]()
  private implicit val formats: DefaultFormats.type = DefaultFormats
  private val rmqClient = new BasicRabbitMQClient(configuration)

  rmqClient.listenQueue(configuration.queueName, (body, ack) => buffer.push((currentOffset.getAndIncrement(), body, ack)))

  override def readSchema(): StructType = schema

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new MicroBatchStream {
      override def latestOffset(): Offset = new LongOffset(currentOffset.get())

      override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
        val startIdx: Long = start.asInstanceOf[LongOffset].offset
        val endIdx: Long = end.asInstanceOf[LongOffset].offset
        val res = JavaConverters.asScalaIterator(buffer.iterator()).takeWhile(p => p._1 > startIdx && p._1 <= endIdx).map(t => t._2).toList
        Array(new RabbitMQInputPartition(res))
      }

      override def createReaderFactory(): PartitionReaderFactory = (partition: InputPartition) => new RabbitMQPartitionReader(partition)

      override def initialOffset(): Offset = new LongOffset(0)

      override def deserializeOffset(json: String): Offset = new LongOffset(parse(json).extract[Long])

      override def commit(end: Offset): Unit = {
        val endOffset = deserializeOffset(end.json()).asInstanceOf[LongOffset]
        val iterator = buffer.descendingIterator()
        breakable {
          while (iterator.hasNext) {
            val (id, _, removeFct): (Long, String, () => Unit) = iterator.next()
            if (id < endOffset.offset) {
              removeFct()
              iterator.remove()
            } else {
              break;
            }
          }
        }
      }

      override def stop(): Unit = {
        rmqClient.stop()
      }
    }
  }
}
