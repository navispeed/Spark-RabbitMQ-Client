package eu.navispeed.rabbitmq

import client.RabbitMQConfiguration

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters

case class Configuration(hostname: String = "localhost", port: Integer = 5672, user: String = "guest",
                         password: String = "guest", virtualHost: String = "/", useSsl: Boolean = false,
                         prefetchCount: Integer = 0, queueName: String) extends RabbitMQConfiguration() {
}

object Configuration {
  private val DEFAULT = new Configuration(queueName = "")

  def from(options: CaseInsensitiveStringMap): Configuration = {

    new Configuration(
      options.getOrDefault("hostname", DEFAULT.hostname),
      options.getInt("port", DEFAULT.port),
      options.getOrDefault("user", DEFAULT.user),
      options.getOrDefault("password", DEFAULT.password),
      options.getOrDefault("virtualHost", DEFAULT.virtualHost),
      options.getBoolean("useSsl", DEFAULT.useSsl),
      options.getInt("prefetchCount", DEFAULT.prefetchCount),
      options.getOrDefault("queueName", DEFAULT.queueName),
    )
  }

  def from(options: Map[String, String]): Configuration = {
    from(new CaseInsensitiveStringMap(JavaConverters.mapAsJavaMap(options)))
  }
}