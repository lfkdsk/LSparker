package io.dashbase.spark.rdds

import java.util

import io.dashbase.spark.apis.{DashbaseSparkCodec, ResponseMerger, TimesliceQuerier, TimesliceSelector}

import scala.collection.JavaConverters._
import scala.util.Random

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

// mock all documents in map.
object Documents {
  lazy val allDocuments: List[MockSparkScoreDoc] = (0 to 100)
    .map(i => new MockSparkScoreDoc(
      i,
      Set(i.toString),
      Set(mockDocItem()))
    )
    .toList

  val random = new Random()

  def mockDocItem() = new MockSparkDoc(Map("title" -> random.nextString(10), "content" -> random.nextString(30)))
}

// mock time slice.
case class MockSelector() extends TimesliceSelector[String] {
  override def apply(t: String): util.Set[String] = Documents.allDocuments.indices.map(i => i.toString).toSet.asJava
}

// mock time querier.
case class MockTimeQuerier() extends TimesliceQuerier[String, MockSparkScoreDoc] {
  override def query(request: String, timeslices: util.Set[String]): MockSparkScoreDoc = {
    timeslices.asScala.map(id => Documents.allDocuments(id.toInt)).reduce((_1, _2) => _1 ++ _2)
  }
}

// mock time response merger.
case class MockMerger() extends ResponseMerger[MockSparkScoreDoc] {
  override def merge(resultSet: util.Set[MockSparkScoreDoc]): MockSparkScoreDoc = {
    resultSet.asScala.reduce((_1, _2) => _1 ++ _2)
  }
}

case class MockDashbaseCodec() extends DashbaseSparkCodec[String, MockSparkScoreDoc] {
  override def timesliceSelector(): TimesliceSelector[String] = MockSelector()

  override def responseMerger(): ResponseMerger[MockSparkScoreDoc] = MockMerger()

  override def timesliceQuerier(): TimesliceQuerier[String, MockSparkScoreDoc] = MockTimeQuerier()
}

// make dashbase codec as params.
class MockSparkDocCodec(query: String = "")
  extends SparkDocCodec[String, MockSparkScoreDoc](query, MockDashbaseCodec()) with Serializable {
}
