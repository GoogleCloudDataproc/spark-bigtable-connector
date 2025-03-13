package com.google.cloud.spark.bigtable.join

import com.google.cloud.spark.bigtable.datasources._
import com.google.cloud.spark.bigtable.{Logging, ReadRowConversions, UserAgentInformation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import com.google.api.gax.rpc.ServerStream
import com.google.cloud.bigtable.data.v2.models.{Query, TableId, Row => BigtableRow}
import com.google.protobuf.ByteString

object BigtableJoinImplicitCommon extends Serializable with Logging {
  private val JOIN_TYPE = "join.type"
  private val COLUMNS_REQUIRED = "columns.required"
  private val PARTITION_COUNT = "partition.count"
  private val BATCH_ROWKEY_SIZE = "batch.rowKeySize"
  private val DEFAULT_JOIN_TYPE = "inner"
  private val DEFAULT_BATCH_SIZE = "25000"
  private val RANGE_PARTITION_ENABLED = "range.partition.enabled"
  private val DEFAULT_RANGE_PARTITION_ENABLED = "true"
  private val ALIAS_NAME = "alias.name"
  private val DEFAULT_ALIAS_NAME = ""
  private val SUPPORTED_JOIN_LIST = List("inner", "left", "anti", "semi")

  /** Joins a DataFrame with Bigtable based on the provided parameters.
    *
    * This function fetches data from Bigtable and joins it with a source DataFrame using either
    * The join type can be "inner", "left", "anti", "semi".
    *
    * ### Example Usage:
    * {{{
    * val joinConfig: Map[String, String] = Map(
    *     "spark.bigtable.project.id" -> projectId, // Required
    *     "spark.bigtable.instance.id" -> instanceId, // Required
    *     "catalog" -> catalog, // Required
    *     "join.type" -> "left",  // Options: "inner", "left" (default: "inner")
    *     "columns.required" -> "order_id,product_id,amount", // Optional
    *     "range.partition.enabled" -> "false", // Default is true. If already partitioned, set to "false"
    *     "partition.count" -> "1000", // Optional
    *     "batch.rowKeySize" -> "10000",  // Default batch size is 25K
    *     "alias.name" -> "bt" // Optional, can be used in join column expression
    * )
    * val joinedDf = df.as("src").joinWithBigtable(joinConfig, "col1", $"src.col1" === $"bt.col2")
    * }}}
    *
    * @param params      Configuration parameters for the Bigtable join.
    * @param srcRowKeyCol Column name in the source DataFrame used for joining.
    * @param usingColumns   Optional join condition. Can be a Column expression or a sequence of column names.
    * @param spark       Implicit Spark session.
    * @return            A DataFrame resulting from the join operation.
    */
  def joinWithBigtable(
      leftDf: DataFrame,
      params: Map[String, String],
      srcRowKeyCol: String,
      usingColumns: Any = ""
  )(implicit spark: SparkSession): DataFrame = {

    // Extract required parameters
    val joinType = params.getOrElse(JOIN_TYPE, DEFAULT_JOIN_TYPE).toLowerCase
    if (!SUPPORTED_JOIN_LIST.exists(joinType.startsWith)) {
      throw new RuntimeException(
        s"This function (joinWithBigtable) supports only these joins: $SUPPORTED_JOIN_LIST"
      )
    }
    val requiredColumns = extractRequiredColumns(params)
    val rangePartitionEnabled =
      params.getOrElse(RANGE_PARTITION_ENABLED, DEFAULT_RANGE_PARTITION_ENABLED).toBoolean
    val aliasName = params.getOrElse(ALIAS_NAME, DEFAULT_ALIAS_NAME)
    // Create Bigtable catalog and schema
    val catalog = BigtableTableCatalog(params)
    val bigtableSchema = constructBigtableSchema(catalog, requiredColumns)

    // Determine partitioning strategy
    val sortedSrcDf = if (rangePartitionEnabled) {
      val partitionNumber = params
        .get(PARTITION_COUNT)
        .map(_.toInt)
        .getOrElse(leftDf.rdd.getNumPartitions)
      leftDf.repartitionByRange(partitionNumber, col(srcRowKeyCol))
    } else leftDf

    // Fetch Bigtable data
    val btDf = fetchBigtableData(params, sortedSrcDf, srcRowKeyCol, bigtableSchema, catalog)
    val btAliasDf = if (aliasName.isEmpty) btDf else btDf.as(aliasName)
    // Perform the join based on the specified condition
    usingColumns match {
      case joinExpr: Array[String] => sortedSrcDf.join(btAliasDf, joinExpr, joinType)
      case joinExpr: Seq[String]   => sortedSrcDf.join(btAliasDf, joinExpr, joinType)
      case joinExpr: Column        => sortedSrcDf.join(btAliasDf, joinExpr, joinType)
      case joinExpr: String =>
        if (joinExpr.isEmpty) sortedSrcDf.join(btAliasDf, srcRowKeyCol, joinType)
        else sortedSrcDf.join(btAliasDf, joinExpr, joinType)
    }
  }


  /** Extracts the required column names from parameters, returns an empty array if none are specified. */
  private def extractRequiredColumns(parameters: Map[String, String]): Array[String] = {
    parameters.getOrElse(COLUMNS_REQUIRED, "").split(",").map(_.trim).filter(_.nonEmpty)
  }

  /** Constructs the schema for Bigtable based on the required columns.
    *
    * @param catalog        The Bigtable catalog containing schema mappings.
    * @param requiredColumns Array of columns required in the output schema.
    * @return StructType representing the final schema.
    */
  private def constructBigtableSchema(
      catalog: BigtableTableCatalog,
      requiredColumns: Array[String]
  ): StructType = {
    val structFields = if (requiredColumns.nonEmpty) {
      requiredColumns.map { columnName =>
        val field = catalog.sMap.getField(columnName)
        StructField(field.sparkColName, field.dt)
      }
    } else catalog.sMap.toFields.toArray

    StructType(structFields)
  }

  /** Fetches data from Bigtable for the given DataFrame's join keys.
    *
    * This method extracts row keys from the source DataFrame, queries Bigtable in batches,
    * and returns the matching rows as a DataFrame.
    *
    * @param parameters     Configuration map for Bigtable connection and batch settings.
    * @param sortedSrcDf    Source DataFrame, pre-sorted by join keys.
    * @param srcRowKeyCol    Columns used as row keys for lookup in Bigtable.
    * @param bigtableSchema Expected schema of the Bigtable data.
    * @param catalog        Bigtable table catalog containing schema mappings.
    * @param spark          Implicit Spark session.
    * @return               DataFrame with data fetched from Bigtable.
    */
  private def fetchBigtableData(
      parameters: Map[String, String],
      sortedSrcDf: DataFrame,
      srcRowKeyCol: String,
      bigtableSchema: StructType,
      catalog: BigtableTableCatalog
  )(implicit spark: SparkSession): DataFrame = {

    // Extract required column names and configure Bigtable client
    val (clientKey, orderedFields, batchSize) =
      getBigtableConfig(parameters, bigtableSchema, catalog)

    // Fetch Bigtable rows based on join keys from the source DataFrame
    val bigtableRdd = sortedSrcDf
      .select(srcRowKeyCol)
      .rdd
      .mapPartitionsWithIndex { (partitionIndex, rows) =>
        val rowKeys = rows.map(_.getAs[String](srcRowKeyCol))
        val btRows =
          fetchBigtableRows(rowKeys, clientKey, catalog.name, partitionIndex, batchSize)
        btRows.map(row => ReadRowConversions.buildRow(orderedFields, row, catalog))
      }

    // Convert RDD to DataFrame and return
    spark.createDataFrame(bigtableRdd, bigtableSchema)
  }

  /** Extracts and initializes the Bigtable client configuration.
    *
    * @param parameters Configuration parameters for Bigtable.
    * @param bigtableSchema Schema of the Bigtable table.
    * @param catalog Bigtable catalog containing schema mappings.
    * @return A tuple containing:
    *         - BigtableClientKey for authentication and interaction.
    *         - Array of ordered fields mapped from the schema.
    *         - Batch size for row key processing.
    */
  private def getBigtableConfig(
      parameters: Map[String, String],
      bigtableSchema: StructType,
      catalog: BigtableTableCatalog
  ): (BigtableClientKey, Array[Field], Int) = {
    val requiredCols = bigtableSchema.map(_.name).toArray
    val btConfig = BigtableSparkConfBuilder().fromMap(parameters).build()
    val clientKey = new BigtableClientKey(btConfig, UserAgentInformation.RDD_TEXT)
    val orderedFields = requiredCols.map(catalog.sMap.getField)
    val batchSize = parameters.getOrElse(BATCH_ROWKEY_SIZE, DEFAULT_BATCH_SIZE).toInt
    (clientKey, orderedFields, batchSize)
  }

  /** Fetches rows from a Bigtable table based on the provided row keys.
    *
    * @param rowKeys        Array of row keys to be fetched.
    * @param clientKey      The Bigtable client configuration key.
    * @param tableId        The ID of the Bigtable table to query.
    * @param pNumber        The partition number (for logging purposes).
    * @param rowKeyBatchSize The size of each batch of row keys for querying Bigtable.
    * @return An iterator of BigtableRow objects containing the retrieved rows.
    */
  private def fetchBigtableRows(
      rowKeys: Iterator[String],
      clientKey: BigtableClientKey,
      tableId: String,
      pNumber: Int,
      rowKeyBatchSize: Int
  ): Iterator[BigtableRow] = {
    try {
      val rowKeyBatches = rowKeys.grouped(rowKeyBatchSize)
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
      case e: Exception =>
        val errorMsg =
          s"Failed to fetch Bigtable rows for table '$tableId' in partition $pNumber. " +
            s"RowKeyBatchSize: $rowKeyBatchSize, Total RowKeys: ${rowKeys.length}. " +
            s"Error: ${e.getMessage}"
        logError(errorMsg, e)
        throw new RuntimeException(errorMsg, e)
    }
  }

  /** Converts a Bigtable ServerStream into an Iterator. */
  private def convertStreamToIterator(
      stream: ServerStream[BigtableRow],
      clientHandle: BigtableDataClientBuilder.DataClientHandle
  ): Iterator[BigtableRow] = {
    val rowStreamIterator = stream.iterator()
    new Iterator[BigtableRow] {
      override def hasNext: Boolean = {
        if (!rowStreamIterator.hasNext) {
          stream.cancel()
          clientHandle.close()
          false
        } else true
      }
      override def next(): BigtableRow = rowStreamIterator.next()
    }
  }
}
