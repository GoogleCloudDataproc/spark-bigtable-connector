/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spark.bigtable.datasources

import com.google.cloud.spark.bigtable.Logging
import com.google.cloud.spark.bigtable.catalog.{CatalogDefinition, RowKey}
import com.google.cloud.bigtable.data.v2.models.{RowMutationEntry, Row => BigtableRow}
import com.google.protobuf.ByteString
import com.google.re2j.Pattern
import org.apache.avro.Schema
import org.apache.spark.sql.{Row => SparkRow}
import org.apache.spark.sql.types._
import org.apache.yetus.audience.InterfaceAudience

import java.sql.{Date, Timestamp}
import scala.collection.mutable

// As we finalize the encoding of different types, we add support for them.
object SupportedDataTypes {
  final val SUPPORTED_TYPES = Array[DataType](
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    BinaryType
  )
}

// This corresponds to the mapping between a DataFrame column and Bigtable column/row key part.
@InterfaceAudience.Private
case class Field(
    sparkColName: String,
    btColFamily: String,
    btColName: String,
    // `simpleType` should be specified for types such as int, string, etc.,
    // as opposed to List, Struct (where Avro should be used).
    simpleType: Option[String] = None,
    avroSchema: Option[String] = None,
    len: Int = -1
) extends Logging {
  override def toString = s"$sparkColName $btColFamily $btColName"
  val isRowKey = btColFamily == BigtableTableCatalog.rowKey

  def schema: Option[Schema] = avroSchema.map { x =>
    logDebug(s"avro: $x")
    val p = new Schema.Parser
    p.parse(x)
  }

  lazy val exeSchema = schema

  // converter from avro to catalyst structure
  lazy val avroToCatalyst: Option[Any => Any] = {
    schema.map(SchemaConverters.createConverterToSQL)
  }

  // converter from catalyst to avro
  lazy val catalystToAvro: (Any) => Any = {
    SchemaConverters.createConverterToAvro(dt, sparkColName, "recordNamespace")
  }

  val dt: DataType = getDt

  private def getDt: DataType = {
    val potentiallySimpleType = simpleType.map(DataTypeParserWrapper.parse)
    if (
      potentiallySimpleType.nonEmpty
      && !SupportedDataTypes.SUPPORTED_TYPES.contains(potentiallySimpleType.get)
    ) {
      throw new IllegalArgumentException(
        "DataType " + potentiallySimpleType.get
          + " is currently not supported for DataFrame columns. Consider converting it to"
          + " a byte array manually first."
      )
    }
    potentiallySimpleType.getOrElse {
      schema
        .map { x =>
          SchemaConverters.toSqlType(x).dataType
        }
        .getOrElse(
          throw new IllegalArgumentException(
            "Invalid catalog definition for column " + sparkColName
              + ". Providing column type or Avro schema is required."
          )
        )
    }
  }

  var length: Int = {
    if (len == -1) {
      dt match {
        case BinaryType | StringType => -1
        case BooleanType             => DataTypeBytes.BOOLEAN_BYTES
        case ByteType                => DataTypeBytes.BYTE_BYTES
        case DoubleType              => DataTypeBytes.DOUBLE_BYTES
        case FloatType               => DataTypeBytes.FLOAT_BYTES
        case IntegerType             => DataTypeBytes.INT_BYTES
        case LongType                => DataTypeBytes.LONG_BYTES
        case ShortType               => DataTypeBytes.SHORT_BYTES
        case _                       => -1
      }
    } else {
      len
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Field =>
      sparkColName == that.sparkColName && btColFamily == that.btColFamily && btColName == that.btColName
    case _ => false
  }

  // This will be cleaned up in an upcoming pr
  def bigtableToScalaValue(cellValue: Array[Byte], offset: Int, len: Int): Any = {
    if (exeSchema.isDefined) {
      // If we have avro schema defined, use it to get record, and then convert them to catalyst data type
      val m = AvroSerdes.deserialize(cellValue, exeSchema.get)
      val n = avroToCatalyst.map(_(m))
      n.get
    } else {
      // Fall back to atomic type
      dt match {
        case BooleanType   => BytesConverter.toBoolean(cellValue, offset)
        case ByteType      => cellValue(offset)
        case ShortType     => BytesConverter.toShort(cellValue, offset)
        case IntegerType   => BytesConverter.toInt(cellValue, offset)
        case LongType      => BytesConverter.toLong(cellValue, offset)
        case FloatType     => BytesConverter.toFloat(cellValue, offset)
        case DoubleType    => BytesConverter.toDouble(cellValue, offset)
        case DateType      => new Date(BytesConverter.toLong(cellValue, offset))
        case TimestampType => new Timestamp(BytesConverter.toLong(cellValue, offset))
        case StringType =>
          BytesConverter.toString(cellValue.slice(offset, offset + len))
        case BinaryType =>
          val newArray = new Array[Byte](len)
          System.arraycopy(cellValue, offset, newArray, 0, len)
          newArray
        // TODO: SparkSqlSerializer.deserialize[Any](src)
        case _ => throw new Exception(s"unsupported data type ${dt}")
      }
    }
  }

  def scalaValueToBigtable(input: Any): Array[Byte] = {
    if (schema.isDefined) {
      // Here we assume the top level type is structType
      val record = catalystToAvro(input)
      AvroSerdes.serialize(record, schema.get)
    } else {
      dt match {
        case BooleanType => BytesConverter.toBytes(input.asInstanceOf[Boolean])
        case ByteType    => Array(input.asInstanceOf[Number].byteValue)
        case ShortType =>
          BytesConverter.toBytes(input.asInstanceOf[Number].shortValue)
        case IntegerType =>
          BytesConverter.toBytes(input.asInstanceOf[Number].intValue)
        case LongType =>
          BytesConverter.toBytes(input.asInstanceOf[Number].longValue)
        case FloatType =>
          BytesConverter.toBytes(input.asInstanceOf[Number].floatValue)
        case DoubleType =>
          BytesConverter.toBytes(input.asInstanceOf[Number].doubleValue)
        case DateType | TimestampType =>
          BytesConverter.toBytes(input.asInstanceOf[java.util.Date].getTime)
        case StringType => BytesConverter.toBytes(input.toString)
        case BinaryType => input.asInstanceOf[Array[Byte]]
        case _          => throw new Exception(s"unsupported data type ${dt}")
      }
    }
  }
}

// The map between the column presented to Spark and the Bigtable field
@InterfaceAudience.Private
case class SchemaMap(map: mutable.HashMap[String, Field], cqMap: mutable.HashMap[String, Field]) {
  def toFields: Seq[StructField] = {
    val regexColumnFields = cqMap.map { case (name, field) =>
      StructField(name, MapType(StringType, field.dt))
    }.toSeq
    val staticColumnFields = map.map { case (name, field) =>
      StructField(name, field.dt)
    }.toSeq
    staticColumnFields ++ regexColumnFields
  }

  def fields = map.values ++ cqMap.values

  def getField(name: String) = (map ++ cqMap)(name)
}

// The definition of Bigtable and Relation relation schema
@InterfaceAudience.Private
case class BigtableTableCatalog(
    name: String,
    row: RowKey,
    sMap: SchemaMap,
    @transient params: Map[String, String]
) extends Logging {
  def toDataType: StructType = StructType(sMap.toFields)
  def getField(name: String): Field = sMap.getField(name)
  def getColumnFamilies: Seq[String] = {
    sMap.fields
      .map(_.btColFamily)
      .filter(_ != BigtableTableCatalog.rowKey)
      .toSeq
      .distinct
  }

  def get(key: String): Option[String] = params.get(key)

  def convertBtRowToSparkRow(btRow: BigtableRow, requestedSparkCols: Seq[String]): SparkRow = {
    val rowKeyFields = row.parseFieldsFromBtRow(btRow)
    val requestedStaticColFields = requestedSparkCols
      .flatMap(sMap.map.get)
      .filter(!_.isRowKey)
      .map(field => {
        val cells = btRow.getCells(field.btColFamily, field.btColName)
        if (cells.isEmpty) {
          (field, None)
        } else {
          val latestCell = cells.get(0).getValue.toByteArray
          if (field.length != -1 && field.length != latestCell.length) {
            throw new IllegalArgumentException(
              "The byte array in Bigtable cell [" + latestCell.mkString(
                ", "
              ) +
                "] has length " + latestCell.length + ", while column " + field +
                " requires length " + field.length + "."
            )
          }

          (
            field,
            field.bigtableToScalaValue(latestCell, 0, latestCell.length)
          )
        }
      }).toMap

    val requestedRegexColFields = requestedSparkCols
      .flatMap(sMap.cqMap.get)
      .filter(!_.isRowKey)
      .map(field => {
        val pattern: Pattern = Pattern.compile(field.btColName)
        val cells = ReadRowUtil.getAllCells(btRow, field.btColFamily)
          .filter(cell => pattern.matcher(cell.getQualifier.toStringUtf8).find())

        if (cells.isEmpty) {
          (field, Seq())
        } else {
          val latestCells = cells
            .groupBy(cell => cell.getQualifier.toStringUtf8)
            .map(_._2.head.getValue.toByteArray)

          (
            field,
            latestCells.map(cell => {
              if (field.length != -1 && cell.length != field.length) {
                throw new IllegalArgumentException(
                  "The byte array in Bigtable cell [" + cell.mkString(
                    ", "
                  ) +
                    "] has length " + cell.length + ", while column " + field +
                    " requires length " + field.length + "."
                )
              }
              field.bigtableToScalaValue(cell, 0, cell.length)
            }).toSeq
          )
        }
      }).toMap

    val combinedFields = (rowKeyFields ++ requestedStaticColFields ++ requestedRegexColFields)
      .map(fieldAndValue => (fieldAndValue._1.sparkColName, fieldAndValue._2))
    SparkRow.fromSeq(
      requestedSparkCols
        .map(col => combinedFields.getOrElse(col, null))
    )
  }

  def createMutationsForSparkRow(sparkRow: SparkRow, schema: StructType, writeTimestampMicros: Long): RowMutationEntry = {
    val rowKeyBytes: Seq[Byte] = row.getBtRowKeyBytes(sparkRow, schema)
    val mutation = RowMutationEntry.create(ByteString.copyFrom(rowKeyBytes.toArray))
    sMap.fields
      .filter(!_.isRowKey)
      .map(field => {
        val fieldIndex = schema.fieldIndex(field.sparkColName)
        (field, sparkRow(fieldIndex))
      })
      .foreach {
        case (field, fieldValue) =>
          val cellValue = field.scalaValueToBigtable(fieldValue)
          val columnNameBytes = BytesConverter.toBytes(field.btColName)
          mutation.setCell(
            field.btColFamily,
            ByteString.copyFrom(columnNameBytes),
            writeTimestampMicros,
            ByteString.copyFrom(cellValue)
          )
      }

    mutation
  }
}

@InterfaceAudience.Public
object BigtableTableCatalog {
  val tableCatalog: String = CatalogDefinition.CATALOG_KEY
  // The row key with format key1:key2 specifying table row key
  val rowKey: String = CatalogDefinition.ROW_KEY
  // The key for Bigtable table whose value specify table name (and potential future settings)
  val table: String = CatalogDefinition.TABLE.KEY
  // The name of Bigtable table
  val tableName: String = CatalogDefinition.TABLE.TABLE_NAME_KEY
  // The name of columns in Bigtable catalog
  val columns: String = CatalogDefinition.COLUMNS.KEY
  val cf: String = CatalogDefinition.COLUMNS.COLUMN_FAMILY_KEY
  val col: String = CatalogDefinition.COLUMNS.COLUMN_QUALIFIER_KEY
  val `type`: String = CatalogDefinition.COLUMNS.TYPE_KEY
  // the name of avro schema json string
  val avro: String = CatalogDefinition.COLUMNS.AVRO_TYPE_NAME_KEY
  val delimiter: Byte = 0
  val length: String = CatalogDefinition.COLUMNS.LENGTH_KEY
  val regexColumns: String = CatalogDefinition.REGEX_COLUMNS.KEY
  val pattern: String = CatalogDefinition.REGEX_COLUMNS.REGEX_PATTERN_KEY

  /** User provide table schema definition
   * {
   *   "tablename": "name",
   *   "rowkey": "key1:key2",
   *   "columns": {
   *     "col1": {
   *       "cf": "cf1",
   *       "col": "col1",
   *       "type": "type1"
   *     },
   *     "col2": {
   *       "cf": "cf2",
   *       "col": "col2",
   *       "type": "type2"
   *     }
   *   }
   * }
   * Note that any col in the rowKey, there has to be one corresponding col defined in columns
   */
  def apply(params: Map[String, String]): BigtableTableCatalog = {
    val catalogDefinition = CatalogDefinition(params)

    val schemaMap = mutable.HashMap.empty[String, Field]
    catalogDefinition.columns.foreach {
      case (name, column) =>
        val f = Field(
          name,
          column.cf.getOrElse(rowKey),
          column.col,
          column.`type`,
          column.avro.map(params(_)),
          column.length.map(_.toInt).getOrElse(-1)
        )
        schemaMap.+=((name, f))
    }

    val cqSchemaMap = mutable.HashMap.empty[String, Field]
    catalogDefinition.regexColumns.foreach(_.foreach {
      case (name, column) =>
        val f = Field(
          name,
          column.cf,
          column.pattern,
          column.`type`,
          column.avro.map(params(_)),
          column.length.map(_.toInt).getOrElse(-1)
        )
        cqSchemaMap.+=((name, f))
    })
    val rKey = RowKey(catalogDefinition.rowkey, schemaMap.filter(_._2.isRowKey).values.toSeq)
    BigtableTableCatalog(
      catalogDefinition.table.name,
      rKey,
      SchemaMap(schemaMap, cqSchemaMap),
      params)
  }
}
