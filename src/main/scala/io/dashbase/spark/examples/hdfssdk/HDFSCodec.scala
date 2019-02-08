package io.dashbase.spark.examples.hdfssdk

import java.util

import io.dashbase.spark.apis.{DashbaseSparkCodec, ResponseMerger, TimesliceQuerier, TimesliceSelector}
import io.dashbase.spark.examples.models.FileQueryResult
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.spark.SparkContext
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
class HDFSCodec(sc: SparkContext) extends DashbaseSparkCodec[String, FileQueryResult] {

  case class HDFSTimeSelector() extends TimesliceSelector[String] {
    override def apply(queryString: String): util.Set[String] = {
      val fileSystem = FileSystem.get(sc.hadoopConfiguration)
      val path = new Path(sc.getConf.get(Constants.FILE_SYSTEM_SEARCH_PATH))
      val iter = fileSystem.listFiles(path, true)
      if (iter == null) {
        throw new IllegalAccessException("cannot access file system")
      }

      val resultPaths = new util.HashSet[String]
      for (status: LocatedFileStatus <- iter) {
        resultPaths.add(status.getPath.toString)
      }

      resultPaths
    }
  }

  case class HDFSResponseMerger() extends ResponseMerger[FileQueryResult] {
    override def merge(resultSet: util.Set[FileQueryResult]): FileQueryResult = ???
  }

  case class HDFSTimeQuerier() extends TimesliceQuerier[String, FileQueryResult] {
    override def query(request: String, timeslices: util.Set[String]): FileQueryResult = {
      timeslices.asScala.map(path => (path, sc.textFile(path)))
        .filter(fileStatus => fileStatus._2.first().contains(request))
        .foreach(fileStatus => {

        })

      FileQueryResult()
    }
  }

  override def timesliceSelector(): TimesliceSelector[String] = HDFSTimeSelector()

  override def responseMerger(): ResponseMerger[FileQueryResult] = HDFSResponseMerger()

  override def timesliceQuerier(): TimesliceQuerier[String, FileQueryResult] = HDFSTimeSelector()
}