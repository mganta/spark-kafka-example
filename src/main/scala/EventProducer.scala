import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import scala.util.Random
import java.util.HashMap

object EventProducer extends App {
  
  val brokers = "localhost:9092"
  val topic = "sometopic"
  
  val props = new HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)

  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  // Send events at 0.1 second intervals
  while (true) {
    val event = newRandomEvent

    val message = new ProducerRecord[String, String](topic, null, event)

    producer.send(message)
    println("Event sent")

    Thread.sleep(100)
  }

  // Generate a random click event
  def newRandomEvent: String = {
    val val1 = Random.nextInt(15)
    val val2 = Random.nextInt(25)
    new String(val1 + "," + val2)
  }

}
