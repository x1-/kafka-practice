package com.inkenkun.x1.kafka.practice

import java.io.{File => JavaFile}
import scala.io.Source

import kafka.producer.NewShinyProducer
import kafka.tools.ConsoleProducer
import kafka.tools.ConsoleProducer.ProducerConfig

object Main {

  val identifier = this.getClass.getCanonicalName

  def main( args: Array[String] ) {

    if ( args.length < 3 ) {
      usage()
      System.exit( -1 )
    }

    val encoding = "UTF-8"
    val fileDir  = args.head

    try {
      val config   = new ProducerConfig( args.tail )
      val props    = ConsoleProducer.getNewProducerProps( config )
      props.put( "metadata.fetch.timeout.ms", "1000" )

      val producer = new NewShinyProducer( props )

      Runtime.getRuntime.addShutdownHook( new Thread() {
        override def run() {
          producer.close()
        }
      } )

      val files = getListOfFiles( fileDir )
      println( "# target file #" )
      println( files.mkString( "\n" ) + "\n" )

      var counter = 0

      files.foreach{ path =>
        val file = Source.fromFile( path, encoding )
        file.getLines.foreach{ line =>
          if ( counter == Int.MaxValue ) counter = 0
          counter = counter + 1
          producer.send( config.topic, Array( counter.toByte ), line.getBytes( encoding ) )
        }
      }
    } catch {
      case e: joptsimple.OptionException =>
        System.err.println(e.getMessage)
        System.exit(1)
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
    System.exit(0)
  }

  def usage() = {
    println( s"Usage: $identifier <fileDir> --topic=topic_name --broker-list=localhost:9092 --timeout=1000 --request-timeout-ms=1000 --request-required-acks=1" )
    println(  "  fileDir     : a directory which has sending files. ex) /var/data" )
    println(  "  topic       : toipc name." )
    println(  "  broker-list : The broker list string. ex) HOST1:PORT1,HOST2:PORT2" )
  }

  private def getListOfFiles( dir: String ): List[JavaFile] = {
    val d = new JavaFile( dir )
    if ( d.exists ) {
      var files = d.listFiles.filter( _.isFile ).toList
      d.listFiles.filter( _.isDirectory ).foreach{ dir =>
        files = files ::: getListOfFiles( dir.getAbsolutePath )
      }
      files
    }
    else
      List[JavaFile]()
  }
}
