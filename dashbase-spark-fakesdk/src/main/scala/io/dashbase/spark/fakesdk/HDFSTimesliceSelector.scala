package io.dashbase.spark.fakesdk

import java.util
import java.util.logging.Filter

import io.dashbase.spark.basesdk.TimesliceSelector
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.EqualTo
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
case class HDFSTimesliceSelector() extends TimesliceSelector {
  override def timeSliceSelector(sqlContext: SQLContext, path: String, filters: util.List[Filter]): util.Set[String] = {
    println("load timeSlice selector")

    val data = sqlContext.read.textFile(path + "meta/index.csv").collect()
    var total = data.apply(0).mkString.split('|')
    val index = data.drop(1).filter(i => i.mkString.split(',').length == 2).map(i => {
      val t = i.mkString.split(',')
      t.apply(0) -> t.apply(1).split('|')
    }).toMap

    filters.asScala.foreach {
      case EqualTo(attr, value) =>
        println("EqualTo filter is used!!" + "Attribute: " + attr + " Value: " + value)
        if (attr == "id") {
          total = index.getOrElse(value.toString, total)
        }

      case f => println("filter: " + f.toString + " is not implemented by us!!")
    }

    println("scan files: " + total.mkString("[", ", ", "]"))
    println("end timeSlice selector")
    total.toSet.asJava
  }
}
