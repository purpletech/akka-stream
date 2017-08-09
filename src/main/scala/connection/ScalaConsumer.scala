package connection
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{StringDeserializer}
import scala.collection.JavaConverters._


object ScalaConsumer {
    import java.util.Properties
    import java.util.Arrays
  def main(ars: Array[String]) = {

    val topic = "test"
    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
  
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "group1")
    props.put("enable.auto.commit", "false");
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Arrays.asList("test"))
    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) println(record.value)
    }
  }
}