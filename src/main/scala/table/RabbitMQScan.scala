package eu.navispeed.rabbitmq
package table

import com.rabbitmq.client._
import net.liftweb.json._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.types.StructType

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters
import scala.util.control.Breaks._

class RabbitMQScan(schema: StructType, configuration: Configuration) extends Scan {

  private val factory = new ConnectionFactory()
  private val connection = factory.newConnection()
  private val channel: Channel = connection.createChannel()
  private val currentOffset = new AtomicLong()
  private val buffer = new ConcurrentLinkedDeque[(Long, String, () => Unit)]()
  private implicit val formats: DefaultFormats.type = DefaultFormats

  override def readSchema(): StructType = schema

  channel.basicConsume("test", false, new DefaultConsumer(channel) {
    override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
      buffer.push((currentOffset.incrementAndGet(), new String(body), () => channel.basicAck(envelope.getDeliveryTag, false)))
    }
  })

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
        channel.close()
        connection.close()
      }
    }
  }
}
