package eu.navispeed.rabbitmq

import table.RabbitMQTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class RabbitMQSource() extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = RabbitMQTable.SCHEMA

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = new RabbitMQTable(SparkSession.active)
}

object RabbitMQSource {
  val name: String = getClass.getCanonicalName.dropRight(1)
}
