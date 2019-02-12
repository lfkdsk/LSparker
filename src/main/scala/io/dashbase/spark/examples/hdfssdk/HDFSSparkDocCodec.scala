package io.dashbase.spark.examples.hdfssdk

import io.dashbase.spark.examples.models.FileQueryResult
import io.dashbase.spark.rdds.SparkDocCodec

class HDFSSparkDocCodec(query: String = "", codec: HDFSCodec)
  extends SparkDocCodec[String, FileQueryResult](query, codec) with Serializable {
}
