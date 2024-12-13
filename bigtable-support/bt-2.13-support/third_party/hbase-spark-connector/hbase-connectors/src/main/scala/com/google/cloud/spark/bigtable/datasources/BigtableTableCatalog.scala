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
import org.apache.avro.Schema
import org.apache.spark.sql.types._
import org.apache.yetus.audience.InterfaceAudience

import scala.collection.mutable
import scala.util.parsing.json.JSON

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
  var start: Int = _
  def schema: Option[Schema] = avroSchema.map { x =>
    logDebug(s"avro: $x")
    val p = new Schema.Parser
    p.parse(x)
  }

  lazy val exeSchema = schema

  // converter from avro to catalyst structure
  lazy val avroToCatalyst: Option[Any => Any] = {
    schema.map(SchemaConverters.createConverterToSQL(_))
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
}

// The row key definition, with each key refer to the col defined in Field, e.g.,
// key1:key2:key3
@InterfaceAudience.Private
case class RowKey(k: String) {
  val keys = k.split(":")
  var fields: Seq[Field] = _
  var varLength = false
  def length = {
    if (varLength) {
      -1
    } else {
      fields.foldLeft(0) { case (x, y) =>
        x + y.length
      }
    }
  }
}
// The map between the column presented to Spark and the Bigtable field
@InterfaceAudience.Private
case class SchemaMap(map: mutable.HashMap[String, Field]) {
  def toFields = map.map { case (name, field) =>
    StructField(name, field.dt)
  }.toSeq

  def fields = map.values

  def getField(name: String) = map(name)
}

// The definition of Bigtable and Relation relation schema
@InterfaceAudience.Private
case class BigtableTableCatalog(
    name: String,
    row: RowKey,
    sMap: SchemaMap,
    @transient params: Map[String, String]
) extends Logging {
  def toDataType = StructType(sMap.toFields)
  def getField(name: String) = sMap.getField(name)
  // One or more columns from the DataFrame get concatenated to turn into
  // the Bigtable row key. This returns a list of those columns.
  def getRowKeyColumns: Seq[Field] = row.fields
  def hasCompoundRowKey: Boolean = (getRowKeyColumns.size > 1)
  def getColumnFamilies = {
    sMap.fields
      .map(_.btColFamily)
      .filter(_ != BigtableTableCatalog.rowKey)
      .toSeq
      .distinct
  }

  def get(key: String) = params.get(key)

  private def initRowKey() = {
    val fields =
      sMap.fields.filter(_.btColFamily == BigtableTableCatalog.rowKey)
    if (fields.isEmpty || row.keys.isEmpty) {
      throw new IllegalArgumentException(
        "No DataFrame columns defined as row key."
      )
    }
    if (fields.size != row.keys.length) {
      throw new IllegalArgumentException(
        "Row key specified in the catalog has "
          + row.keys.length
          + " columns, while "
          + fields.size
          + " columns are defined as row key."
      )
    }
    row.fields = row.keys.flatMap(n => fields.find(_.btColName == n))
    // The length is determined at run time if it is string or binary and the length is undefined.
    if (row.fields.filter(_.length == -1).isEmpty) {
      var start = 0
      row.fields.foreach { f =>
        f.start = start
        start += f.length
      }
    } else {
      row.varLength = true
    }
  }

  private def validateCompoundRowKey(): Unit = {
    if (!hasCompoundRowKey) {
      return
    }
    var i = 0
    row.fields.foreach { field =>
      if (field.length == -1) {
        field.dt match {
          case BinaryType => {
            if (i != row.fields.length - 1) {
              throw new IllegalArgumentException(
                "Error with DataFrame column " + field + ". "
                  + "When using compound row keys, a binary type should have a "
                  + "fixed length or be the last column in the appended row key."
              )
            }
          }
          case StringType =>
          case _ =>
            throw new IllegalArgumentException(
              "Error with DataFrame column " + field + ". Type " + field.dt
                + " is not accepted when using compound row keys."
            )
        }
      }
      i += 1
    }
  }

  initRowKey()
  validateCompoundRowKey()
}

@InterfaceAudience.Public
object BigtableTableCatalog {
  val tableCatalog = "catalog"
  // The row key with format key1:key2 specifying table row key
  val rowKey = "rowkey"
  // The key for Bigtable table whose value specify table name (and potential future settings)
  val table = "table"
  // The name of Bigtable table
  val tableName = "name"
  // The name of columns in Bigtable catalog
  val columns = "columns"
  val cf = "cf"
  val col = "col"
  val `type` = "type"
  // the name of avro schema json string
  val avro = "avro"
  val delimiter: Byte = 0
  val length = "length"

  /** User provide table schema definition
    * {"tablename":"name", "rowkey":"key1:key2",
    * "columns":{"col1":{"cf":"cf1", "col":"col1", "type":"type1"},
    * "col2":{"cf":"cf2", "col":"col2", "type":"type2"}}}
    * Note that any col in the rowKey, there has to be one corresponding col defined in columns
    */
  def apply(params: Map[String, String]): BigtableTableCatalog = {
    val parameters = params
    val jString = parameters(tableCatalog)
    val map = JSON.parseFull(jString).get.asInstanceOf[Map[String, _]]
    val tableMeta = map(table).asInstanceOf[Map[String, _]]
    val tName = tableMeta(tableName).asInstanceOf[String]
    val cIter = map(columns)
      .asInstanceOf[Map[String, Map[String, String]]]
      .toIterator
    val schemaMap = mutable.HashMap.empty[String, Field]
    cIter.foreach { case (name, column) =>
      val len = column.get(length).map(_.toInt).getOrElse(-1)
      val sAvro = column.get(avro).map(parameters(_))
      val f = Field(
        name,
        column.getOrElse(cf, rowKey),
        column(col),
        column.get(`type`),
        sAvro,
        len
      )
      schemaMap.+=((name, f))
    }
    val rKey = RowKey(map.get(rowKey).get.asInstanceOf[String])
    BigtableTableCatalog(tName, rKey, SchemaMap(schemaMap), parameters)
  }

  /* Use the json based definition formated as below
       (currently only long, string, and binary are supported)
    val catalog = s"""{
                      |"table":{"name":"bttable"},
                      |"rowkey":"key1:key2",
                      |"columns":{
                      |"col1":{"cf":"rowkey", "col":"key1", "type":"string"},
                      |"col2":{"cf":"rowkey", "col":"key2", "type":"double"},
                      |"col3":{"cf":"cf1", "col":"col2", "type":"binary"},
                      |"col4":{"cf":"cf1", "col":"col3", "type":"timestamp"},
                      |"col5":{"cf":"cf1", "col":"col4", "type":"double"},
                      |"col6":{"cf":"cf1", "col":"col5", "type":"$map"},
                      |"col7":{"cf":"cf1", "col":"col6", "type":"$array"},
                      |"col8":{"cf":"cf1", "col":"col7", "type":"$arrayMap"}
                      |}
                      |}""".stripMargin
   */
}

/** Construct to contains column data that spend SparkSQL and Bigtable
  *
  * @param columnName   SparkSQL column name
  * @param colType      SparkSQL column type
  * @param columnFamily Bigtable column family
  * @param qualifier    Bigtable qualifier name
  */
@InterfaceAudience.Private
case class SchemaQualifierDefinition(
    columnName: String,
    colType: String,
    columnFamily: String,
    qualifier: String
)
