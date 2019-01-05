package com.lfkdsk.selectors

import java.nio.file.Paths

import com.lfkdsk.selector.{AbstractSegmentSelector, TimeRange}
import org.apache.lucene.index.{DirectoryReader, SegmentInfo, SegmentReader}
import org.apache.lucene.search.{IndexSearcher, Query}
import org.apache.lucene.store.FSDirectory

import scala.reflect.ClassTag
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
class DirSegmentSelector(path: String) extends AbstractSegmentSelector[SegmentReader] {
  override implicit protected def kTag: ClassTag[SegmentReader] = ???

  override def size: Long = ???

  override def iterator: Iterator[SegmentReader] = ???

  override def select(timeRange: TimeRange, query: Query): List[SegmentReader] = {
    val indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(path)))
    indexReader.leaves().asScala.map(ctx => ctx.reader().asInstanceOf[SegmentReader]).toList
  }

  override def close(): Unit = ???
}
