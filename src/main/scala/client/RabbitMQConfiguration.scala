package eu.navispeed.rabbitmq
package client

private [rabbitmq] trait RabbitMQConfiguration {
  def hostname: String
  def port: Integer
  def user: String
  def password: String
  def virtualHost: String
  def useSsl: Boolean
  def prefetchCount: Integer
}
