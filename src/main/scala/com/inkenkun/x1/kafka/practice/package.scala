package com.inkenkun.x1.kafka

import java.util.Properties
import scala.collection.JavaConverters._

import com.typesafe.config.Config

package object practice {

  implicit class Config2Prop( self: Config ) {
    def toProp: Properties = {
      self.entrySet.asScala.foldLeft( new Properties ) { ( prop, entry ) =>
        prop.put( entry.getKey, self.getString( entry.getKey ) )
        prop
      }
    }
  }
}
