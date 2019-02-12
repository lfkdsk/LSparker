package io.dashbase.spark.fakesdk

import io.dashbase.spark.basesdk.SchemaFetcher
import io.dashbase.spark.basesdk.utils.SchemaUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, StructType}

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
/**
  * Read StructType from schema.
  */
case class HDFSSchemaFetcher() extends SchemaFetcher {
  override def fetchSchema(sqlContext: SQLContext, path: String): StructType = {
    println("load schema file")
    val df = sqlContext.read.json(path + "meta/schema.json")
    val fields = df.collect().map(t =>
      StructField(
        t.getString(t.fieldIndex("name")),
        SchemaUtil.schemaTo(t.getString(t.fieldIndex("type"))))
    )

    println("end load schema")
    StructType(fields)
  }
}
