package eu.navispeed.rabbitmq
package table

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class RabbitMQScanBuilder(sparkSession: SparkSession, schema: StructType, options: CaseInsensitiveStringMap) extends ScanBuilder {

  override def build(): Scan = {
    new RabbitMQScan(schema, Configuration.from(options))
  }
}
