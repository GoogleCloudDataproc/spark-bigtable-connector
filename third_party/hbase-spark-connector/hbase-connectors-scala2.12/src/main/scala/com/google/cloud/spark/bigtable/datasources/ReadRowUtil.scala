package com.google.cloud.spark.bigtable.datasources

import com.google.cloud.bigtable.data.v2.models.{RowCell, Row => BigtableRow}

import scala.collection.JavaConverters.asScalaBufferConverter

object ReadRowUtil extends Serializable {

  def getAllCells(bigtableRow: BigtableRow, btColFamily: String): List[RowCell] = {
    bigtableRow.getCells(btColFamily).asScala.toList
  }

  def getAllCells(
      bigtableRow: BigtableRow,
      btColFamily: String,
      btColName: String
  ): List[RowCell] = {
    bigtableRow.getCells(btColFamily, btColName).asScala.toList
  }

}
