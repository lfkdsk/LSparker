package io.dashbase.spark.sql

import io.dashbase.spark.basesdk.DashbaseSparkCodec
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

import scala.util.{Success, Try}

class DefaultSource extends RelationProvider with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val codec = loadSDKClazz(parameters.get("sdk").toString, sqlContext)
    val path = parameters.get("path")

    path match {
      case Some(p) => new DashbaseRelation(p, parameters, codec, sqlContext, Option(schema))
      case _ => throw new IllegalArgumentException("Path is required for datasource format!!")
    }
  }

  def loadSDKClazz(codecClazzPath: String, sqlCon: SQLContext): SparkCodecWrapper = {
    val loader = Thread.currentThread().getContextClassLoader
    Try(loader.loadClass(codecClazzPath)) match {
      case Success(dataSource) => new SparkCodecWrapper(dataSource.newInstance().asInstanceOf[DashbaseSparkCodec], sqlCon)
      case _ => throw new IllegalArgumentException("Codec Path is required for datasource format!!")
    }
  }
}
