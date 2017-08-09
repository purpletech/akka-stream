package connection
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka._
import akka.actor.{Props, ActorRef, Actor, ActorSystem, ActorLogging, PoisonPill}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.ActorMaterializer
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait TestProducer {
  val system = ActorSystem("example")
  implicit val ec = system.dispatcher
  implicit val m = ActorMaterializer.create(system)
  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
}
object ReactiveKafkaProducer extends TestProducer {
  def main(args: Array[String]) = {
    var tag = 0
    val tick = Source.tick(1.second, 5.second, ("hi-" + (tag = tag +1)))
    val done = tick.map { elem =>
      new ProducerRecord[Array[Byte], String]("test", elem)
    }
    .runWith(Producer.plainSink(producerSettings))
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