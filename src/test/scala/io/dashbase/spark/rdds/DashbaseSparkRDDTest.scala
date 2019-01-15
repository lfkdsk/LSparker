package io.dashbase.spark.rdds

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.log4j.{Level, LogManager}
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
class DashbaseSparkRDDTest extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext
  with Serializable {

  override val conf: SparkConf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)

  override protected def beforeEach(): Unit = LogManager.getRootLogger.setLevel(Level.INFO)

  "dashbaseRDD.collectRes()" should "run in series partitions" in {
    val codec = new MockSparkDocCodec()
    val dashbaseRDD = new DashbaseSparkRDD(sc, codec, 10)
    val res = dashbaseRDD.collectRes()
    res.score should be((0 to 100).sum)
  }

  "dashbaseRDD.take(Int)" should "get top N entites in series partitions" in {
    val codec = new MockSparkDocCodec()
    val dashbaseRDD = new DashbaseSparkRDD(sc, codec, 10)
    val res = dashbaseRDD.take(5)
    res.length should be(5)
  }
}
