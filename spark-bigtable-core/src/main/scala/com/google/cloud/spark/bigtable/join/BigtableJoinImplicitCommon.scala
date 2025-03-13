package com.google.cloud.spark.bigtable.join

import com.google.api.core.{ApiFuture, ApiFutures}
import com.google.cloud.spark.bigtable.datasources._
import com.google.cloud.spark.bigtable.{Logging, ReadRowConversions, UserAgentInformation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession, Row => SparkRow}
import com.google.api.gax.rpc.ServerStream
import com.google.cloud.bigtable.data.v2.models.{Query, TableId, Row => BigtableRow}
import com.google.protobuf.ByteString

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter

object BigtableJoinImplicitCommon extends Serializable with Logging {
  private val SUPPORTED_JOIN_LIST = List("inner", "left", "anti", "semi")

  /** Joins a DataFrame with Bigtable based on the provided parameters.
    *
    * This function fetches data from Bigtable and joins it with a source DataFrame using either
    * The join type can be "inner", "left", "anti", "semi".
    *
    * ### Example Usage:
    * {{{*+
    *
    * val joinConfig: Map[String, String] = Map(
    *     "spark.bigtable.project.id" -> projectId, // Required
    *     "spark.bigtable.instance.id" -> instanceId, // Required
    *     "catalog" -> catalog, // Required
    * )
    * val joinedDf = df.as("src").joinWithBigtable(joinConfig, "col1", $"src.col1" === $"bt.col2")
    * }}}
    *
    * @param params         Configuration parameters for the Bigtable join.
    * @param srcRowKeyCol   Column name in the source DataFrame used for joining.
    * @param usingColumns   Optional join condition. Can be a Column expression or a sequence of column names.
    * @param joinType       Optional join type. Can be left, anti, semi, inner.
    * @param aliasName      Optional bigtable df alias name.
    * @param spark          Implicit Spark session.
    * @return               A DataFrame resulting from the join operation.
    */
  def joinWithBigtable(
      leftDf: DataFrame,
      params: Map[String, String],
      srcRowKeyCol: String,
      usingColumns: Any = "",
      joinType: String = "inner",
      aliasName: String = ""
  )(implicit spark: SparkSession): DataFrame = {
    // Extract required parameters
    if (!SUPPORTED_JOIN_LIST.exists(joinType.toLowerCase.startsWith)) {
      throw new RuntimeException(
        s"This function (joinWithBigtable) supports only these joins: $SUPPORTED_JOIN_LIST"
      )
    }
    // Create Bigtable catalog and schema
    val catalog = BigtableTableCatalog(params)

    // Fetch Bigtable data
    val btDf = fetchBigtableData(params, leftDf, srcRowKeyCol, catalog)
    val btAliasDf = if (aliasName.isEmpty) btDf else btDf.as(aliasName)
    // Perform the join based on the specified condition
    usingColumns match {
      case joinExpr: Array[String] => leftDf.join(btAliasDf, joinExpr, joinType)
      case joinExpr: Seq[String]   => leftDf.join(btAliasDf, joinExpr, joinType)
      case joinExpr: Column        => leftDf.join(btAliasDf, joinExpr, joinType)
      case joinExpr: String =>
        if (joinExpr.isEmpty) leftDf.join(btAliasDf, srcRowKeyCol, joinType)
        else leftDf.join(btAliasDf, joinExpr, joinType)
    }
  }

  /** Fetches data from Bigtable for the given DataFrame's join keys.
    *
    * This method extracts row keys from the source DataFrame, queries Bigtable in batches,
    * and returns the matching rows as a DataFrame.
    *
    * @param parameters     Configuration map for Bigtable connection and batch settings.
    * @param srcDf          Source DataFrame, pre-sorted by join keys.
    * @param srcRowKeyCol   Columns used as row keys for lookup in Bigtable.
    * @param catalog        Bigtable table catalog containing schema mappings.
    * @param spark          Implicit Spark session.
    * @return               DataFrame with data fetched from Bigtable.
    */
  private def fetchBigtableData(
      parameters: Map[String, String],
      srcDf: DataFrame,
      srcRowKeyCol: String,
      catalog: BigtableTableCatalog
  )(implicit spark: SparkSession): DataFrame = {
    val bigtableSchema = StructType(catalog.sMap.toFields.toArray)
    // Extract required column names and configure Bigtable client
    val (clientKey, orderedFields) =
      getBigtableConfig(parameters, bigtableSchema, catalog)
    val btRowKeyField = catalog.sMap.fields.find(_.isRowKey).get
    val srcRowKeyField = btRowKeyField.copy(sparkColName = srcRowKeyCol)
    // Fetch Bigtable rows based on join keys from the source DataFrame
    val bigtableRdd = srcDf
      .select(srcRowKeyCol)
      .rdd
      .mapPartitionsWithIndex { (partitionIndex, rows) =>
        val rowKeys = rows.map(r => Utils.toBytes(r.getAs[Any](srcRowKeyCol), srcRowKeyField))
        val btRows = fetchBigtableRows(rowKeys, clientKey, catalog.name, partitionIndex)
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
    */
  private def getBigtableConfig(
      parameters: Map[String, String],
      bigtableSchema: StructType,
      catalog: BigtableTableCatalog
  ): (BigtableClientKey, Seq[Field]) = {
    val requiredCols = bigtableSchema.map(_.name)
    val btConfig = BigtableSparkConfBuilder().fromMap(parameters).build()
    val clientKey = new BigtableClientKey(btConfig, UserAgentInformation.DIRECT_JOINS_TEXT)
    val orderedFields = requiredCols.map(catalog.sMap.getField)
    (clientKey, orderedFields)
  }

  /** Fetches rows from a Bigtable table based on the provided row keys.
    *
    * @param rowKeys        Array of row keys to be fetched.
    * @param clientKey      The Bigtable client configuration key.
    * @param tableId        The ID of the Bigtable table to query.
    * @param pNumber        The partition number (for logging purposes).
    * @return An iterator of BigtableRow objects containing the retrieved rows.
    */

  private def fetchBigtableRows(
      rowKeys: Iterator[Array[Byte]],
      clientKey: BigtableClientKey,
      tableId: String,
      pNumber: Int
  ): Iterator[BigtableRow] = {
    try {
      val rows: util.List[ApiFuture[BigtableRow]] = new util.ArrayList[ApiFuture[BigtableRow]]()
      val clientHandle = BigtableDataClientBuilder.getHandle(clientKey)
      val bigtableClient = clientHandle.getClient()
      val batcher = bigtableClient.newBulkReadRowsBatcher(TableId.of(tableId))
      rowKeys.foreach { rowKey =>
        val rowKeyBytes = ByteString.copyFrom(rowKey)
        val rowFuture = batcher.add(rowKeyBytes)
        rows.add(rowFuture)
      }
      batcher.close()
      val btRows = ApiFutures.allAsList(rows).get().asScala.iterator
      clientHandle.close()
      btRows
    } catch {
      case e: Exception =>
        val errorMsg =
          s"Failed to fetch Bigtable rows for table '$tableId' in partition $pNumber. " +
            s"Total RowKeys: ${rowKeys.length}. " +
            s"Error: ${e.getMessage}"
        logError(errorMsg, e)
        throw new RuntimeException(errorMsg, e)
    }
  }
}
