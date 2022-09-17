package eu.navispeed.rabbitmq
package table

import table.RabbitMQTable.SCHEMA

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class RabbitMQTable(sparkSession: SparkSession) extends Table with SupportsRead {
  override def name(): String = "rabbitmq"

  override def schema(): StructType = SCHEMA

  override def capabilities(): util.Set[TableCapability] = util.Set.of(TableCapability.MICRO_BATCH_READ, TableCapability.CONTINUOUS_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new RabbitMQScanBuilder(sparkSession, schema(), options)
  }
}

object RabbitMQTable {
  val SCHEMA: StructType = StructType.apply(Array(StructField("json", DataTypes.StringType)))
}