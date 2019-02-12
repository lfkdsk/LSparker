
package io.dashbase.spark.basesdk.utils

import org.apache.spark.sql.types.{TimestampType, DataType, LongType, IntegerType, StringType}

object SchemaUtil {
  def castTo(value: String, dataType: DataType) = {
    dataType match {
      case _: IntegerType => value.toInt
      case _: LongType => value.toLong
      case _: StringType => value
    }
  }

  def schemaTo(value: String): DataType = {
    value match {
      case "long" => LongType
      case "timestamp" => TimestampType
      case "string" => StringType
    }
  }
}
