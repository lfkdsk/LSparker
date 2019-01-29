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

import sbt._
import Keys._

name := "dashbase-spark"
scalaVersion := "2.11.12"
version := "0.1"
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-language:implicitConversions"
)

lazy val settings = Seq(
  scalacOptions ++= Seq(
    "-unchecked",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-deprecation",
    "-encoding",
    "utf8"
  )
)

lazy val dependencies =
  new {
    val sparkCore = "org.apache.spark" %% "spark-core" % "2.4.0"
    val sparkSql = "org.apache.spark" %% "spark-sql" % "2.4.0"
    val luceneCore = "org.apache.lucene" % "lucene-core" % "7.6.0"
    val luceneQueryParser = "org.apache.lucene" % "lucene-queryparser" % "7.6.0"
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % "test"
    val sparkTest = "com.holdenkarau" %% "spark-testing-base" % s"2.3.1_0.10.0" % "test" intransitive()
  }

lazy val commonDependencies = Seq(
  dependencies.sparkCore,
  dependencies.sparkSql,
  dependencies.scalaTest,
  dependencies.sparkTest
)

lazy val global = project.in(file("."))
  .settings(
    settings,
    libraryDependencies ++= commonDependencies
  )

lazy val example = project.settings(settings).dependsOn(global)