package com.lfkdsk

import com.lfkdsk.partitions.AbstractLucenePartition
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}
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

  override def compute(split: Partition, context: TaskContext): Iterator[T] = ???

  override protected def getPartitions: Array[Partition] = ???
}
