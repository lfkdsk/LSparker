package io.dashbase.spark.sql

import io.dashbase.spark.basesdk.DashbaseSparkCodec
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class SparkCodecWrapper(codec: DashbaseSparkCodec, sqlCon: SQLContext) {
  if (codec == null) {
    throw new IllegalArgumentException("codec could not been null")
  }

  private val schemaFetcher = codec.schemaFetcher()
  private val timeSliceSelector = codec.timesliceSelector()
  private val timeSliceQuerier = codec.timesliceQuerier()

  if (schemaFetcher == null || timeSliceSelector == null || timeSliceQuerier == null) {
    throw new IllegalArgumentException("some params could not been null.")
  }

  def fetchSchema(): StructType = {
    schemaFetcher.fetchSchema(sqlCon)
  }

  def selectSlices(): Set[String] = {
    timeSliceSelector.timeSliceSelector(sqlCon).asScala.toSet
  }

  def query(query: String, segments: Set[String]): Row = {
    timeSliceQuerier.query(query, segments.asJava)
  }
}
