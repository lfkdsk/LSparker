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
package io.dashbase.spark.rdds

/**
  * Wrapper around Lucene document
  *
  * @param doc Lucene document
  */
class MockSparkDoc(doc: Map[String, String]) extends Serializable {

  override def toString: String = {
    val builder = new StringBuilder
    doc.foreach { case (name, value) =>
      builder.append(s"$name:[${value}]")
    }
    builder.result()
  }
}

object MockSparkDoc extends Serializable {
  def apply(doc: Map[String, String]): MockSparkDoc = {
    new MockSparkDoc(doc)
  }
}

class MockSparkScoreDoc(var score: Float, var docId: Set[String], var doc: Set[MockSparkDoc]) extends Serializable {
  override def toString: String = {
    val builder = new StringBuilder
    builder.append(s"[score: ${score}/")
    builder.append(s"docId: ${docId}/")
    builder.append(s"doc: ${doc}]")
    builder.result()
  }

  def ++(other: MockSparkScoreDoc): MockSparkScoreDoc = {
    this.docId ++= other.docId
    this.score += other.score
    this.doc ++= other.doc
    this
  }
}

