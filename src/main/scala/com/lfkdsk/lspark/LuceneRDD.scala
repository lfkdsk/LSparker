package com.lfkdsk.lspark

import com.lfkdsk.lspark.partitions.{AbstractLucenePartition, LocalLucenePartition}
import com.lfkdsk.lspark.response.{LucenePartitionResponse, LuceneResponse}
import com.lfkdsk.lspark.selectors.DirSegmentSelector
import org.apache.lucene.index.{IndexReader, SegmentReader}
import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

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
class LuceneRDD[T: ClassTag](partitions: RDD[AbstractLucenePartition[T]])
  extends RDD[T](partitions.context, List(new OneToOneDependency(partitions))) {

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    firstParent[AbstractLucenePartition[T]].iterator(split, context).next.iterator
  }

  override protected def getPartitions: Array[Partition] = partitions.partitions

  protected def partitionMap(factor: AbstractLucenePartition[T] => LucenePartitionResponse)
  : LuceneResponse = {
    new LuceneResponse(partitions.map(factor))
  }

  def query(searchString: String,
            topK: Int = 10): LuceneResponse = {
    partitionMap(_.query(searchString, topK))
  }
}

object LuceneRDD extends Serializable {
  def apply[T: ClassTag](elems: Iterable[T])(implicit sc: SparkContext): LuceneRDD[T] = {
    val partitions = sc.parallelize[T](elems.toSeq).map[AbstractLucenePartition[T]] {
      case segmentReader: SegmentReader => new LocalLucenePartition(segmentReader)
        .asInstanceOf[AbstractLucenePartition[T]]
      case _ => null
    }

    new LuceneRDD[T](partitions)
  }
}