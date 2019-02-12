package io.dashbase.spark.fakesdk

import java.util

import io.dashbase.spark.basesdk.utils.SchemaUtil
import io.dashbase.spark.basesdk.{DashbaseConstants, TimesliceQuerier}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

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
case class HDFSTimesliceQuerier() extends TimesliceQuerier {
  override def query(sqlContext: SQLContext, schema: StructType, query: String, timeslices: util.Set[String]): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val path = sqlContext.getConf(DashbaseConstants.DASHBASE_SEARCH_PATH)
    val files = timeslices.asScala.toSet
    val schemaFields = schema.fields

    if (path == null)
      throw new IllegalArgumentException("need search path as params in conf with key: [ " + DashbaseConstants.DASHBASE_SEARCH_PATH + " ]")

    val rdds: Set[RDD[String]] = files.map(f => {
      sc.textFile(new Path(path, f).toString, files.size)
    })

    val rdd = rdds.reduce((_1, _2) => _1 ++ _2)
    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)
      val tmp = data.map(words => words.zipWithIndex.map {
        case (value, index) =>
          val colName = schemaFields(index).name
          SchemaUtil.castTo(
            if (colName.equalsIgnoreCase("gender")) {
              if (value.toInt == 1)
                "Male"
              else
                "Female"
            } else
              value,
            schemaFields(index).dataType
          )
      })

      tmp.map(s => Row.fromSeq(s))
    })

    rows.flatMap(e => e)
  }
}
