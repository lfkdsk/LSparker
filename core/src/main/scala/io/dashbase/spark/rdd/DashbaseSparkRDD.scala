package io.dashbase.spark.rdd

import io.dashbase.spark.sql.SparkCodecWrapper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, SparkException, TaskContext}

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
class DashbaseSparkRDD(sc: SparkContext,
                       protected val codec: SparkCodecWrapper,
                       protected var minPartitions: Int = 2)
  extends RDD[Row](sc, Nil) with AutoCloseable {

  logInfo("Instance is created...")
  setName("Dashbase-RDD")

  // checkCodecNil()
  logInfo("codec to split jobs to partitions.")

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition = split.asInstanceOf[DashbaseSparkRDDPartition]
    logInfo(s"query start for partition in thread ${Thread.currentThread().getId}")
//    Iterator(partition.query(codec.query, query, partition.segments))
  }

  override protected def getPartitions: Array[Partition] = {
    val segments = codec.selectSlices()
    // get min partitions.
    minPartitions = math.min(segments.size, minPartitions)
    val slices = slice(segments, minPartitions).toArray
    slices.indices.map(i => DashbaseSparkRDDPartition(id, i, slices(i).toSet)).toArray
  }

  /**
    * Slice a collection into numSlices sub-collections. One extra thing we do here is to treat Range
    * collections specially, encoding the slices as other Ranges to minimize memory cost. This makes
    * it efficient to run Spark over RDDs representing large sets of numbers. And if the collection
    * is an inclusive Range, we use inclusive range for the last slice.
    */
  def slice[T: ClassTag](seq: Set[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of partitions required")
    }

    // Sequences need to be sliced at the same set of index positions for operations
    // like RDD.zip() to behave as expected
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }

    // TODO : extend for range methods.
    val array = seq.toArray // To prevent O(n^2) operations for List etc
    positions(array.length, numSlices).map { case (start, end) =>
      array.slice(start, end).toSeq
    }.toSeq
  }

  //    def collectRes(): Response = {
  //      logInfo("codec to merge collected partitions' response")
  //      val results = sparkContext.runJob(this, (iter: Iterator[Response]) => iter.toArray, partitions.indices)
  //      codec.merge(Array.concat(results: _*))
  //    }
  //

  private def checkCodecNil(): Unit = {
    if (codec == null) {
      throw new SparkException("This LSpark RDD lakes a codec.")
    }
  }


  override def close(): Unit = {
    logInfo("Closing LuceneRDD...")
  }
}