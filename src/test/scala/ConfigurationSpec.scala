package eu.navispeed.rabbitmq

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.flatspec.AnyFlatSpec

class ConfigurationSpec extends AnyFlatSpec {
  "configuration class" should "be constructable from a Map" in {
    Configuration.fromMap(Map(("hostname", "localhost")))
  }
}
