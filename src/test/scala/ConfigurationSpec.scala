package eu.navispeed.rabbitmq

//import Configuration.toMap

import org.scalatest.flatspec.AnyFlatSpec

class ConfigurationSpec extends AnyFlatSpec {
  "configuration class" should "be constructable from a Map" in {
    Configuration.from(Map(("hostname", "localhost")))
  }

  "configuration class" should "be used as a map" in {
    def test(map: Map[String, String]): Unit = {
      assert(map.size == 8)
    }

    test(new Configuration(queueName = ""))
  }
}
