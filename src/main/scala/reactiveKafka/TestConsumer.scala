package reactiveKafka

import akka.kafka._
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer
import akka.Done
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import scala.concurrent.Future
import scala.util.{Failure, Success}


trait TestConsumer {
  val system = ActorSystem("example")
  implicit val ec = system.dispatcher
  implicit val m = ActorMaterializer.create(system)
  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  
}
object AtMostOnceConsumer extends TestConsumer {
  def main(args: Array[String]): Unit = {
   

    val done = Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("test"))
      .mapAsync(1) { record =>
        println("Got new message: " + record.value)
        Future.successful(Done)
      }
      .runWith(Sink.ignore)
    // #atMostOnce

    terminateWhenDone(done)
  }
  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }
}
object AtLeastOnceConsumer extends TestConsumer {
  def main(args: Array[String]): Unit = {
   

    val done = Consumer.committableSource(consumerSettings, Subscriptions.topics("test"))
      .mapAsync(1) { msg =>
        println("Got new message: " + msg.record.value)
        msg.committableOffset.commitScaladsl()
        Future.successful(Done)
      }
      .runWith(Sink.ignore)
    // #atMostOnce

    terminateWhenDone(done)
  }
  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }
}