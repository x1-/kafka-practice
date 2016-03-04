package com.inkenkun.x1.kafka.practice

import java.lang.{Integer => JavaInt, String => JavaString}

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata, Callback}

case class Producer( topic: String, brokers: String ) {

  val props = ConfigFactory.load.getConfig( "kafka.producer" ).toProp
  props.put( "bootstrap.servers", brokers )
  props.put( "client.id", this.getClass.getCanonicalName )

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