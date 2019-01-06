package com.lfkdsk.lspark

import java.{lang, util}
import java.nio.file.Paths

import com.holdenkarau.spark.testing.SharedSparkContext
import com.lfkdsk.lspark.partitions.{AbstractLucenePartition, LocalLucenePartition}
import com.lfkdsk.lspark.selectors.DirSegmentSelector
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, TextField}
import org.apache.lucene.index._
import org.apache.lucene.store.FSDirectory
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterEach, FlatSpec, FunSuite, Matchers}

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
class LuceneRDDTest extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  override val conf: SparkConf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)

  val testPath = getClass.getResource("segments").getPath

  override protected def beforeEach(): Unit = {
    object FakeMergePolicy extends MergePolicy {
      override def findMerges(mergeTrigger: MergeTrigger, segmentInfos: SegmentInfos, mergeContext: MergePolicy.MergeContext): MergePolicy.MergeSpecification = null

      override def findForcedMerges(segmentInfos: SegmentInfos, maxSegmentCount: Int, segmentsToMerge: util.Map[SegmentCommitInfo, lang.Boolean], mergeContext: MergePolicy.MergeContext): MergePolicy.MergeSpecification = null

      override def findForcedDeletesMerges(segmentInfos: SegmentInfos, mergeContext: MergePolicy.MergeContext): MergePolicy.MergeSpecification = null
    }

    def mockLucene(path: String): Unit = {
      val analyzer = new StandardAnalyzer
      val directory = FSDirectory.open(Paths.get(path))
      // clear test files.
      for (name <- directory.listAll) {
        directory.deleteFile(name)
      }

      val config = new IndexWriterConfig(analyzer).setMergePolicy(FakeMergePolicy)
      val indexWriter = new IndexWriter(directory, config)

      for (i <- 1 to 100) {
        val doc = new Document
        doc.add(new TextField("title", String.valueOf(i), Field.Store.YES))
        doc.add(new TextField("content", String.valueOf(i), Field.Store.YES))

        indexWriter.addDocument(doc)
        if (i % 10 == 0) {
          indexWriter.commit()
        }
      }
      indexWriter.close()
    }

    mockLucene(testPath)
  }

  "LuceneRDDs.query(String)" should "search elements in each segments" in {
    val selector = new DirSegmentSelector(testPath)
    val segments = selector.select(null, null) // default select all segments.

    val partitions = sc.parallelize(segments).map {
      case segmentReader: SegmentReader => new LocalLucenePartition(segmentReader).asInstanceOf[AbstractLucenePartition[SegmentReader]]
      case _ => null
    }
  }
}
