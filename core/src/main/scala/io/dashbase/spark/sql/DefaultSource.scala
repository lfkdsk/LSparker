package io.dashbase.spark.sql

import io.dashbase.spark.basesdk.DashbaseSparkCodec
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import scala.reflect.runtime.{universe => ru}

class DefaultSource extends RelationProvider with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val codec = loadSDKClazz(parameters("sdk"), sqlContext)
    val path = parameters.get("path")

    path match {
      case Some(p) => new DashbaseRelation(p, parameters, codec, sqlContext, Option(schema))
      case _ => throw new IllegalArgumentException("Path is required for datasource format!!")
    }
  }

  def loadSDKClazz(codecClazzPath: String, sqlCon: SQLContext): SparkCodecWrapper = {
    val loader = Thread.currentThread().getContextClassLoader
    val clazz = loader.loadClass(codecClazzPath)
    if (isAssignableFrom(clazz, classOf[DashbaseSparkCodec])) {
      return new SparkCodecWrapper(clazz.newInstance().asInstanceOf[DashbaseSparkCodec], sqlCon)
    }

    throw new IllegalArgumentException("Codec Path is required for datasource format!!")
  }

  def isAssignableFrom[T](scalaClass: Class[_], javaClass: Class[T]): Boolean = {
    val javaClassType: ru.Type = getType(javaClass)
    val scalaClassType: ru.Type = getType(scalaClass)
    scalaClassType.<:<(javaClassType)
  }

  def getType[T](clazz: Class[T]): ru.Type = {
    val runtimeMirror = ru.runtimeMirror(clazz.getClassLoader)
    runtimeMirror.classSymbol(clazz).toType
  }
}
