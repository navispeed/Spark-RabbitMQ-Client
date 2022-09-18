# Spark-RabbitMQ-Client

This project contain a Spark 3.x custom source so that Spark can receive messages from RabbitMQ.

## Using it

```scala
    sparkSession.readStream
      .format(RabbitMQSource.name)
      .options(Configuration(hostname = "localhost", port = 5672, user = "guest", password = "guest", virtualHost = "/", useSsl = false, prefetchCount = 0, queueName = "test"))
      .load()
      .withColumn("value", from_json(col("json"), encoder.schema))
      .select("value.*")
      .as[Model]
      .writeStream
      .foreachBatch(myFunc _)
      .trigger(Trigger.Once())
      .start()
      .awaitTermination()
```

## Testing it

You need to have a local RabbitMQ with the default configuration & to have a queue and an exchange named "test".