package com.inkenkun.x1.kafka.practice

import java.util.Properties
import java.lang.{Integer => JavaInt, String => JavaString}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata, Callback}

case class Producer( topic: String, brokers: String ) {

  val props = new Properties
  props.put( "bootstrap.servers", brokers )
  props.put( "client.id", this.getClass.getCanonicalName )
  props.put( "acks", "all" )
  props.put( "timeout.ms", "100" )
  props.put( "metadata.fetch.timeout.ms", "1000" )
  props.put( "key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer" )
  props.put( "value.serializer", "org.apache.kafka.common.serialization.StringSerializer" )

  val producer = new KafkaProducer[JavaInt, JavaString]( props )

  def send( key: Int, message: String ) =
    producer.send(
      new ProducerRecord[JavaInt, JavaString]( topic, key, message ),
      new PracticeCallback( System.currentTimeMillis(), key, message )
    )

}

class PracticeCallback( startTime: Long, key: Int, message: String ) extends Callback {
  override def onCompletion( metadata: RecordMetadata, exception: Exception ): Unit = {
    val elapsedTime = System.currentTimeMillis() - startTime
    if ( metadata != null ) {
      println(
        s"message( $key, $message ) sent to partition( ${metadata.partition()} ), "
          + s"offset( ${metadata.offset()} ) in $elapsedTime ms"
      )
    } else {
      exception.printStackTrace()
    }
  }
}