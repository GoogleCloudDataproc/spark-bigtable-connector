package com.google.cloud.spark.bigtable.join

import org.apache.spark.sql.{DataFrame, SparkSession}

object BigtableJoinImplicit {
  implicit class DataFrameRich(leftDf: DataFrame) {
    def joinWithBigtable(
        params: Map[String, String],
        srcRowKeyCol: String,
        usingColumns: Any = "",
        joinType: String = "inner",
        aliasName: String = ""
    )(implicit
        spark: SparkSession
    ): DataFrame = {
      BigtableJoinImplicitCommon.joinWithBigtable(
        leftDf,
        params,
        srcRowKeyCol,
        usingColumns,
        joinType,
        aliasName
      )
    }
  }
}
