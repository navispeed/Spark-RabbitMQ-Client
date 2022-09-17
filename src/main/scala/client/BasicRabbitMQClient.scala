package eu.navispeed.rabbitmq
package client

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{AMQP, Channel, ConnectionFactory, DefaultConsumer, Envelope}

class BasicRabbitMQClient(configuration: Configuration) {
  private val factory = new ConnectionFactory()

  factory.setHost(configuration.hostname)
  factory.setPort(configuration.port)
  factory.setUsername(configuration.user)
  factory.setPassword(configuration.password)
  factory.setVirtualHost(configuration.virtualHost)
  if (configuration.useSsl) {
    factory.useSslProtocol()
  }

  private val connection = factory.newConnection()
  private val channel: Channel = connection.createChannel()

  channel.basicQos(configuration.prefetchCount)

  def listenQueue(queueName: String, action: (String, () => Unit) => Unit): Unit = {
    channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
        action(new String(body), () => channel.basicAck(envelope.getDeliveryTag, false))
      }
    })
  }

  def send(exchange: String, body: String): Unit = {
    channel.basicPublish(exchange, body.hashCode.toString, new BasicProperties(), body.getBytes)
  }

  def stop(): Unit = {
    channel.close()
    connection.close()
  }
}
