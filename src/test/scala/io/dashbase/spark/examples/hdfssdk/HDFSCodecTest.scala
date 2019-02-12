package io.dashbase.spark.examples.hdfssdk

import com.holdenkarau.spark.testing.SharedSparkContext
import io.dashbase.spark.examples.models.FileQueryResult
import io.dashbase.spark.rdds.DashbaseSparkRDD
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

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
class HDFSCodecTest extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext
  with Serializable {

  override val conf: SparkConf = new SparkConf().
    setMaster("local[*]").
    setAppName(this.getClass.getName).
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID).
    set(Constants.FILE_SYSTEM_SEARCH_PATH, "/Users/liufengkai/Documents/Code/personal-projects/LSparker/tmp/")

  "hdfsCodec" should "" in {
    val codec = new HDFSCodec()
    codec.initialTimeSelector(sc.hadoopConfiguration, conf)
    val sparkCodec = new HDFSSparkDocCodec("1", codec)
    val dashbaseRDD = new DashbaseSparkRDD[String, FileQueryResult](sc, sparkCodec, 10)
    val res = dashbaseRDD.collectRes()
  }
}
