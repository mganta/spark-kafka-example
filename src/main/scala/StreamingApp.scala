import kafka.serializer.DefaultDecoder
import kafka.serializer.StringDecoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ Minutes, Seconds, StreamingContext }
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.NullWritable
import com.example.avro.SomeEvent

//spark streaming job to read a delimited record from kafka and write to hdfs as avro
object StreamingApp extends App {

  //set kryo
  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "EventRegistrator")

  println("Initializing App")

  //streaming context
  val sparkConf = new SparkConf().setMaster("local[2]").setAppName("EventCount")
  val ssc = new StreamingContext(sparkConf, Seconds(10))

  //get hdfs client api
  val job = new Job()
  AvroJob.setOutputKeySchema(job, SomeEvent.SCHEMA$)
  job.setOutputFormatClass(classOf[AvroKeyOutputFormat[AvroKey[SomeEvent]]])

  //connect to kafka
  val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
  val topics = Set("sometopic")
  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
  
  //split the delimited record
  val parsedEvents = messages.map(_.split(","))

  //convert to avro
  val avroEvents = parsedEvents.map(event => (generateAvroEventArrays(event), NullWritable.get))

  //print data
  avroEvents.print
  
  //optionally, save as a hadoop file
  avroEvents.saveAsNewAPIHadoopFiles("/tmp/output/data", null, classOf[AvroKey[SomeEvent]], classOf[NullWritable], classOf[AvroKeyOutputFormat[SomeEvent]], job.getConfiguration)

  //start streaming
  ssc.start()
  ssc.awaitTermination()
  
    //convert to avro
  def generateAvroEventArrays(incomingEvent: Array[String]): AvroKey[SomeEvent] = {
    val event = new SomeEvent()
    event.setId(incomingEvent(0).trim())
    event.setValue(incomingEvent(1).trim())
    new AvroKey[SomeEvent] (event) 
    }
}
