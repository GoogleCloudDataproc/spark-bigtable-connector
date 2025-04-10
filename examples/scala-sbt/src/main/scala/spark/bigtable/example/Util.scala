package spark.bigtable.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import spark.bigtable.example.WordCount.doubleToBinary

object Util {
  def createExampleBigtable(spark: SparkSession,
                            createNewTable: String,
                            projectId: String,
                            instanceId: String,
                            tableName: String): Unit = {

    val doubleToBinaryUdf = udf((value: Double) => doubleToBinary(value))

    import spark.implicits._
    val data = (0 to 999).map(i => ("word%d".format(i), i, i / 1000.0))
    val rdd = spark.sparkContext.parallelize(data)
    val dfWithDouble = rdd.toDF("word", "count", "frequency_double")

    println("Created the DataFrame:");
    dfWithDouble.show()

    val dfToWrite = dfWithDouble
      .withColumn(
        "frequency_binary",
        doubleToBinaryUdf(dfWithDouble.col("frequency_double"))
      )
      .drop("frequency_double")

    dfToWrite.write
      .format("bigtable")
      .option("catalog", Util.getCatalog(tableName))
      .option("spark.bigtable.create.new.table", createNewTable)
      .option("spark.bigtable.project.id", projectId)
      .option("spark.bigtable.instance.id", instanceId)
      .save
    println("DataFrame was written to Bigtable.")
  }

  def getCatalog(tableName: String): String =
    s"""{
       |"table":{"namespace":"default", "name":"$tableName", "tableCoder":"PrimitiveType"},
       |"rowkey":"wordCol",
       |"columns":{
       |  "word":{"cf":"rowkey", "col":"wordCol", "type":"string"},
       |  "count":{"cf":"example_family", "col":"countCol", "type":"long"},
       |  "frequency_binary":{"cf":"example_family", "col":"frequencyCol", "type":"binary"}
       |}
       |}""".stripMargin

}
