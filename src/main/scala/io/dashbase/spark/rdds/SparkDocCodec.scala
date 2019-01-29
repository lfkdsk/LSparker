package io.dashbase.spark.rdds

import io.dashbase.spark.apis.DashbaseSparkCodec
import org.apache.spark.rdd.RDD

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
class SparkDocCodec[Request, Response](request: Request, codec: DashbaseSparkCodec[Request, Response]) extends Serializable {
  // params null check.
  paramsNullCheck()

  def select(): Set[String] = {
    codec.timesliceSelector().apply(request).asScala.toSet
  }

  def query(segments: Set[String]): Response = {
    codec.timesliceQuerier().query(request, segments.asJava)
  }

  def merge(res: Seq[Response]): Response = {
    codec.responseMerger().merge(res.toSet.asJava)
  }

  private def paramsNullCheck(): Unit = {
    if (request == null || codec == null) {
      throw new IllegalArgumentException(s"params has null values: request: ${request == null}, codec: ${codec == null}")
    }
  }
}
