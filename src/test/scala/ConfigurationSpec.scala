package eu.navispeed.rabbitmq

import org.scalatest.flatspec.AnyFlatSpec

class ConfigurationSpec extends AnyFlatSpec {
  "configuration class" should "be constructable from a Map" in {
    Configuration.from(Map(("hostname", "localhost")))
  }
}
