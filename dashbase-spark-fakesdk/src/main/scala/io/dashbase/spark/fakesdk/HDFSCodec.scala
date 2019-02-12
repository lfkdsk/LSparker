package io.dashbase.spark.fakesdk

import io.dashbase.spark.basesdk.{DashbaseSparkCodec, SchemaFetcher, TimesliceQuerier, TimesliceSelector}

class HDFSCodec extends DashbaseSparkCodec {
  override def schemaFetcher(): SchemaFetcher = HDFSSchemaFetcher()

  override def timesliceSelector(): TimesliceSelector = HDFSTimesliceSelector()

  override def timesliceQuerier(): TimesliceQuerier = ???
}
