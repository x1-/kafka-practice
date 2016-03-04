package com.inkenkun.x1.kafka.practice

import java.io.{File => JavaFile}
import scala.io.Source

object Main {

  val identifier = this.getClass.getCanonicalName

  def main( args: Array[String] ) {

    if ( args.length < 4 ) {
      usage()
      System.exit( -1 )
    }

    val zkHost   = args(0)
    val brokers  = args(1)
    val topic    = args(2)
    val fileDir  = args(3)

    val encoding = "UTF-8"

    val producer = Producer( topic, brokers )

    val files = getListOfFiles( fileDir )
    println( "# target file #" )
    println( files.mkString( "\n" ) + "\n" )

    var counter = 0

    files.foreach{ path =>
      val file = Source.fromFile( path, encoding )
      file.getLines.foreach{ line =>
        if ( counter == Int.MaxValue ) counter = 0
        counter = counter + 1
        producer.send( counter, line )
      }
    }
  }

  def usage() = {
    println( s"Usage: $identifier <zkHost> <brokers> <topic> <fileDir>" )
    println(  "  zkHost  : zookeeper host ex) localhost:2128" )
    println(  "  brokers : comma separated broker server list. ex) localhost:9092,localhost:8083" )
    println(  "  topic   : toipc name." )
    println(  "  fileDir : a directory which has sending files. ex) /var/data" )
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
