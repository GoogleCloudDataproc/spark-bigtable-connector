package com.google.cloud.spark.bigtable.join

import com.google.cloud.spark.bigtable.datasources._
import com.google.cloud.spark.bigtable.{ReadRowConversions, UserAgentInformation}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.google.api.gax.rpc.ServerStream
import com.google.cloud.bigtable.data.v2.models.{Query, TableId, Row => BigtableRow}
import com.google.protobuf.ByteString

object BigtableJoinImplicit extends Serializable {

  implicit class RichDataFrame(leftDf: DataFrame) extends Serializable {

    /** Joins a DataFrame with Bigtable based on the provided parameters.
      *
      * Example usage:
      * {{
      * val joinParameters: Map[String, String] = Map(
      *     "spark.bigtable.batch.mutate.size" -> "5000",
      *     "spark.bigtable.project.id" -> projectId,
      *     "spark.bigtable.instance.id" -> instanceId,
      *     "catalog" -> catalogFor1BTable,
      *     "joinKey" -> joiningColumn,
      *     "joinType" -> "inner"
      * )
      * df.joinWithBigtable(joinParameters)
      * }}
      *
      * @param parameters Configuration parameters for Bigtable join
      * @param spark Implicit Spark session
      * @return Joined DataFrame
      *
      * Note: The joinType should be either "inner" or "left". Any other value will result in a full Bigtable scan.
      */
    def joinWithBigtable(
        parameters: Map[String, String]
    )(implicit spark: SparkSession): DataFrame = {

      // Extract join and required columns from parameters
      val joinColumns = extractJoinColumns(parameters)
      val requiredColumns = extractRequiredColumns(parameters)
      val joinType = parameters("joinType")

      // Create Bigtable catalog and schema
      val catalog = BigtableTableCatalog(parameters)
      val bigtableSchema = constructBigtableSchema(catalog, requiredColumns)

      // Fetch data from Bigtable and perform the join
      val bigtableDf = fetchBigtableData(parameters, joinColumns, bigtableSchema, catalog)
      leftDf.join(bigtableDf, joinColumns, joinType)
    }

    /** Extracts join column names from the provided parameters. */
    private def extractJoinColumns(parameters: Map[String, String]): Array[String] = {
      parameters("joinKey").split(",").map(_.trim)
    }

    /** Extracts the required column names from parameters, returns an empty array if none are specified. */
    private def extractRequiredColumns(parameters: Map[String, String]): Array[String] = {
      parameters.getOrElse("requiredColumns", "").split(",").map(_.trim).filter(_.nonEmpty)
    }

    /** Constructs the schema for the Bigtable DataFrame based on the required columns. */
    private def constructBigtableSchema(
        catalog: BigtableTableCatalog,
        requiredColumns: Array[String]
    ): StructType = {
      val structFields = requiredColumns.map { columnName =>
        val field = catalog.sMap.getField(columnName)
        StructField(field.sparkColName, field.dt)
      }
      if (structFields.isEmpty) catalog.toDataType else StructType(structFields)
    }

    /** Fetches data from Bigtable, converts it into a DataFrame, and returns it. */
    private def fetchBigtableData(
        parameters: Map[String, String],
        joinColumns: Array[String],
        bigtableSchema: StructType,
        catalog: BigtableTableCatalog
    )(implicit spark: SparkSession): DataFrame = {
      val requiredColumns = bigtableSchema.map(_.name).toArray
      val bigtableConfig = BigtableSparkConfBuilder().fromMap(parameters).build()
      val clientKey = new BigtableClientKey(bigtableConfig, UserAgentInformation.RDD_TEXT)
      val orderedFields = requiredColumns.map(catalog.sMap.getField)

      // Extract row keys from left DataFrame and fetch corresponding Bigtable rows
      val bigtableRdd = leftDf.select(joinColumns.map(col): _*).rdd.mapPartitions { partition =>
        val rowKeys = partition.map(_.getString(0))
        val bigtableRows = fetchBigtableRows(rowKeys, clientKey, catalog.name)
        bigtableRows.map(row => ReadRowConversions.buildRow(orderedFields, row, catalog))
      }

      // Convert RDD to DataFrame and return
      spark.createDataFrame(bigtableRdd, bigtableSchema)
    }

    /** Fetches rows from Bigtable based on the given row keys. */
    def fetchBigtableRows(
        rowKeys: Iterator[String],
        clientKey: BigtableClientKey,
        tableId: String
    ): Iterator[BigtableRow] = {
      try {
        val rowKeyBatches = rowKeys.grouped(25000)
        rowKeyBatches.flatMap { batch =>
          val clientHandle = BigtableDataClientBuilder.getHandle(clientKey)
          val bigtableClient = clientHandle.getClient()
          val query = Query.create(TableId.of(tableId))
          batch.foreach { rowKey =>
            val rowKeyBytes = ByteString.copyFrom(BytesConverter.toBytes(rowKey))
            query.rowKey(rowKeyBytes)
          }
          val responseStream = bigtableClient.readRows(query)
          convertStreamToIterator(responseStream, clientHandle)
        }
      } catch {
        case e: Exception => throw e
      }
    }

    /** Converts a Bigtable ServerStream into an Iterator. */
    private def convertStreamToIterator(
        stream: ServerStream[BigtableRow],
        clientHandle: BigtableDataClientBuilder.DataClientHandle
    ): Iterator[BigtableRow] = {
      val iterator = stream.iterator()
      new Iterator[BigtableRow] {
        override def hasNext: Boolean = {
          if (!iterator.hasNext) {
            stream.cancel()
            clientHandle.close()
            false
          } else true
        }
        override def next(): BigtableRow = iterator.next()
      }
    }
  }
}
