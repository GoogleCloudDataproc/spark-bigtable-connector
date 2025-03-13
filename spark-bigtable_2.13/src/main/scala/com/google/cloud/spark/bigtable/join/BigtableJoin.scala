package com.google.cloud.spark.bigtable.join

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}

object BigtableJoin {

  def joinWithBigtable(
      leftDf: DataFrame,
      params: java.util.Map[String, String],
      srcRowKeyCol: String,
      usingColumns: Any = "",
      joinType: String = "inner",
      aliasName: String = ""
  )(implicit
      spark: SparkSession
  ): DataFrame = {
    val paramsAsScala = params.asScala.toMap
    val scalaUsingCols = usingColumns match {
      case joinExpr: java.util.List[_]      => joinExpr.asScala.toSeq
      case joinExpr: java.util.ArrayList[_] => joinExpr.asScala.toSeq
      case joinExpr: Any                    => joinExpr
    }
    BigtableJoinImplicitCommon.joinWithBigtable(
      leftDf,
      paramsAsScala,
      srcRowKeyCol,
      scalaUsingCols,
      joinType,
      aliasName
    )
  }
}
