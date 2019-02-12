package io.dashbase.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

private[sql] class DashbaseRelation(path: String,
                                    parameters: Map[String, String],
                                    codec: SparkCodecWrapper,
                                    val sqlContext: SQLContext,
                                    userSchema: Option[StructType] = None)
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with Serializable {

  private var _schema: Option[StructType] = Option.empty
  private val _codec = codec

  override def schema: StructType = {
    if (userSchema.isDefined) {
      return userSchema.get
    } else if (_schema.isDefined) {
      return _schema.get
    }
    _schema = Option.apply(_codec.fetchSchema())
    _schema.get
  }

  // TableScan
  def buildScan(): RDD[Row] = buildScan(Array.empty)

  // PrunedScan
  def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan(requiredColumns, Array.empty)

  // PrunedFilteredScan
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println("TableScan: buildScan called...")
    val sc = sqlContext.sparkContext
    val schemaFields = schema.fields
    val ids = _codec.selectSlices(filters)
    codec.query(_schema.get, null, ids)
  }
}
