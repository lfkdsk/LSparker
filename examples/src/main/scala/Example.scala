import io.dashbase.spark.basesdk.DashbaseConstants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
object Example extends App {
  println("Application started...")

  val conf = new SparkConf().setAppName("spark-custom-datasource")
    .set(DashbaseConstants.DASHBASE_SEARCH_PATH, "data/")
  val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()

  val df = spark.sqlContext.read
    .format("io.dashbase.spark.sql")
    .option("sdk", "io.dashbase.spark.fakesdk.HDFSCodec")
    .option("header", "false")
    .load("data/")

  //print the schema
  df.printSchema()
  // df.show()

  df.createOrReplaceTempView("test")
  spark.sql("select * from test where id = 10001").show()
  // spark.sql("select * from test where id = 10002").show()
  Thread.sleep(1000000)
  println("Application Ended...")
}
