package com.google.cloud.spark.bigtable.join

import org.apache.spark.sql.{DataFrame, SparkSession}

object BigtableJoinImplicit {
  implicit class DataFrameRich(leftDf: DataFrame) {
    def joinWithBigtable(params: Map[String, String], srcRowKeyCol: String, usingColumns: Any = "")(
        implicit spark: SparkSession
    ): DataFrame = {
      BigtableJoinImplicitCommon.joinWithBigtable(leftDf, params, srcRowKeyCol, usingColumns)
    }
  }
}
