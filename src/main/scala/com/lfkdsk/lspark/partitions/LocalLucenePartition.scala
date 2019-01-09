package com.lfkdsk.lspark.partitions

import com.lfkdsk.lspark.response.LucenePartitionResponse
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexReader, SegmentReader}
import org.apache.lucene.search.{IndexSearcher, Query}

import scala.reflect.ClassTag
import org.apache.lucene.queryparser.classic.QueryParser

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
class LocalLucenePartition(reader: SegmentReader) extends AbstractLucenePartition[SegmentReader] {
  override implicit protected def kTag: ClassTag[SegmentReader] = ClassTag(classOf[SegmentReader])

  override def size: Long = ???

  override def iterator: Iterator[SegmentReader] = ???

  override def fields(): Set[String] = ???

  lazy val indexSearcher = new IndexSearcher(reader)

  override def query(searchString: String, topK: Int): LucenePartitionResponse = {
    val q = parseQueryString(searchString, new StandardAnalyzer())
    LucenePartitionResponse(indexSearcher.search(q, topK).scoreDocs.toIterator)
  }

  def parseQueryString(searchString: String, analyzer: Analyzer): Query = {
    val queryParser = new QueryParser("text", analyzer)
    queryParser.parse(searchString)
  }


  override def close(): Unit = ???
}
