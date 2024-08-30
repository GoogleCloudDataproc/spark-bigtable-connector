# Apache Spark SQL connector for Google Bigtable

This connector allows writing Spark SQL DataFrames
into [Google Bigtable](https://cloud.google.com/bigtable) and reading tables
from Bigtable. It uses
the [Spark SQL Data Source API V1](https://spark.apache.org/docs/latest/sql-data-sources.html)
to connect to Bigtable.

## Quickstart

You can access the connector in two different ways:

1. From
   our [Maven Central repository](https://repo1.maven.org/maven2/com/google/cloud/spark/bigtable).
2. Through a public GCS bucket, located
   at `gs://spark-lib/bigtable/spark-bigtable_2.12-<version>.jar`.

In Java and Scala applications, you can use different dependency management
tools (e.g., Maven, sbt, or Gradle) to access the
connector `com.google.cloud.spark.bigtable:spark-bigtable_2.12:<version>`(
current `<version>` is `0.2.1`) and package it inside your application JAR using
libraries such as Maven Shade Plugin. For PySpark applications, you can use
the `--jars` flag to pass the GCS address of the connector when submitting it.

For Maven, you can add the following snippet to your `pom.xml` file:

```xml
<dependency>
  <groupId>com.google.cloud.spark.bigtable</groupId>
  <artifactId>spark-bigtable_2.12</artifactId>
  <version>0.2.1</version>
</dependency>
```

For sbt, you can add the following to your `build.sbt` file:

```
libraryDependencies += "com.google.cloud.spark.bigtable" % "spark-bigtable_2.12" % "0.2.1"
```

Finally, you can add the following to your `build.gradle` file when using
Gradle:

```
dependencies {
implementation group: 'com.google.cloud.bigtable', name: 'spark-bigtable_2.12', version: '0.2.1'
}
```

Note that you need plugins such
as [Maven Shade Plugin](https://maven.apache.org/plugins/maven-shade-plugin/),
[sbt-assembly](https://github.com/sbt/sbt-assembly), or
[Shadow Plugin](https://imperceptiblethoughts.com/shadow/introduction/) to
package the connector inside
your JAR in Maven, sbt, and Gradle, respectively.

## Getting started

### About Bigtable

[Bigtable](https://cloud.google.com/bigtable) is Google's NoSQL Big Data
database service.
It's the same service powering many of Google's internal applications, e.g.,
Search, Maps, etc. You can refer
to [Bigtable documentations](https://cloud.google.com/bigtable/docs/instances-clusters-nodes)
to learn more about key concepts, including instances, clusters, nodes, and
tablets.

### About Apache Spark and Spark SQL

Apache Spark is a distributed computing framework designed for fast and
large-scale data processing,
where [resilient distributed dataset (RDD)](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
is the main data model. Spark SQL is a module built on top of Spark that
provides a SQL-like interface for querying and manipulating data. This is done
through [DataFrame and DataSet](https://spark.apache.org/docs/latest/sql-programming-guide.html),
Spark SQL's data model, built on top of RDDs.

### Supported Spark runtime environments and requirements

You can use the connector with Spark locally with the
[Bigtable emulator](https://cloud.google.com/bigtable/docs/emulator),
as well as in managed environments such as Dataproc cluster or serverless.
You need the following depending on the environments you choose to use:

| Runtime environment                                                                 | Bigtable | Dataproc | Cloud Storage |
|-------------------------------------------------------------------------------------|----------|----------|---------------|
| Local Spark w/ [Bigtable emulator](https://cloud.google.com/bigtable/docs/emulator) | Optional | Optional | Optional      |
| Local Spark                                                                         | Required | Optional | Optional      |
| Dataproc Cluster                                                                    | Required | Required | Optional      |
| Dataproc Serverless                                                                 | Required | Required | Required      |

### Supported Spark versions

The connector supports the following Spark versions **with Scala 2.12**:

| Scala version | Spark versions                    | Spark Application Languages                             |
|---------------|-----------------------------------|---------------------------------------------------------|
| 2.12          | 2.4.8, 3.1.x, 3.2.x, 3.4.x, 3.5.x | Java, Scala, PySpark (`.py` files or Jupyter notebooks) |

## Main features

For a detailed list of features and how to use them, you can refer the official
documentation
[here](https://cloud.google.com/bigtable/docs/use-bigtable-spark-connector).
A list of main features is as follows:

### Catalog definition

You can define a catalog as a JSON-formatted string, to convert from the
DataFrame's schema to a
format compatible with Bigtable. This is an example of a catalog JSON:

```
{
  "table": {"name": "t1"},
  "rowkey": "id_rowkey",
  "columns": {
    "id": {"cf": "rowkey", "col": "id_rowkey", "type": "string"},
    "name": {"cf": "info", "col": "name", "type": "string"},
    "birthYear": {"cf": "info", "col": "birth_year", "type": "long"},
    "address": {"cf": "location", "col": "address", "type": "string"}
  }
}
```

Here, the columns `name`, `birthYear`, and `address` from the DataFrame are
converted into Bigtable
columns and the `id` column is used as the row key. Note that you could also
specify *compound* row keys,
which are created by concatenating multiple DataFrame columns together.

### Writing to Bigtable

You can use the `bigtable` format along with specifying the Bigtable
project and instance id to write to Bigtable. The catalog definition
specifies the table destination. This is a sample snippet of writing
to Bigtable using Java:

```java
Dataset<Row> dataFrame;
// Adding some values to dataFrame.
dataFrame
  .write()
  .format("bigtable")
  .option("catalog", catalog)
  .option("spark.bigtable.project.id", projectId)
  .option("spark.bigtable.instance.id", instanceId);
```

### Reading from Bigtable

You can use the `bigtable` format and catalog, along with the Bigtable
project and instance id to read from Bigtable. This is a sample snippet
of reading from Bigtable using Java:

```java
Dataset<Row> dataFrame = spark
    .read()
    .format("bigtable")
    .option("catalog", catalog)
    .option("spark.bigtable.project.id", projectId)
    .option("spark.bigtable.instance.id", instanceId)
    .load();
```

### Runtime configurations

You can use `.option(<config_name>, <config_value>)` in Spark to pass different
runtime configurations to
the connector. For example, Bigtable project and instance ID or settings for
timestamp and timeout configurations.
For a full list of configurations, refer to
[BigtableSparkConf.scala](spark-bigtable_2.12/src/main/scala/com/google/cloud/spark/bigtable/datasources/BigtableSparkConf.scala),
where these configs are defined.

### Bigtable emulator support

When using the connector locally, you can start a Bigtable emulator server and
set the environment variable
`export BIGTABLE_EMULATOR_HOST=localhost:<emulator_port>` in the same
environment where Spark is launched. The connector will use the emulator
instead of a real Bigtable instance. You can refer to the
[Bigtable emulator documentations](https://cloud.google.com/bigtable/docs/emulator)
for more details on using it.

### Simple data type serialization

This connector supports encoding a number of Spark's simple data types as byte
array for storage in Bigtable, with the following table summarizing the Spark
type names, the corresponding
[GoogleSQL data types](https://cloud.google.com/spanner/docs/reference/standard-sql/data-types),
the names you need to use in the catalog string, and the encoding description:

| Spark SQL data type | GoogleSQL type | Catalog type | Encoding description                                                                                                        |
|---------------------|----------------|--------------|-----------------------------------------------------------------------------------------------------------------------------|
| `BooleanType`       | `BOOL`         | `boolean`    | `True` -> 1 byte corresponding to `-1` in two's-complement <br/> `False` -> 1 byte corresponding to `0` in two's-complement |
| `ByteType`          | N/A            | `byte`       | 1-byte signed two's-complement number                                                                                       |
| `ShortType`         | N/A            | `short`      | 2-byte signed two's-complement number, using big-endian                                                                     |
| `IntegerType`       | N/A            | `int`        | 4-byte signed two's-complement number, using big-endian                                                                     |
| `LongType`          | `INT64`        | `long`       | 8-byte signed two's-complement number, using big-endian                                                                     |
| `FloatType`         | `FLOAT32`      | `float`      | 4-byte single-precision using IEEE 754 standard                                                                             |
| `DoubleType`        | `FLOAT64`      | `double`     | 64-bit double-precision using IEEE 754 standard                                                                             |
| `StringType`        | `STRING`       | `string`     | UTF-8 encoded string                                                                                                        |
| `BinaryType`        | `BYTES`        | `binary`     | The corresponding array of bytes                                                                                            |

You can refer to
[this link](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
for more information on Spark SQL types.
If you want to use a specific encoding schema for your DataFrame values, you
can manually convert these types to byte arrays and then store them on
Bigtable. The examples in the `examples` folder contain samples for converting
a column to `BinaryType` inside your application, in different languages.

### Complex data type serialization using Apache Avro

You can specify an Avro schema for columns with a complex Spark SQL type such as
`ArrayType`, `MapType`, or `StructType`,
to serialize and store them in Bigtable.

### Row key filter push down

This connector supports pushing down some of the filters on the row key column
in the DataFrame to Bigtable
and performing them on the server-side. The list of supported or non-supported
filters is as follows:

**_NOTE:_**  The supported row key filters are only pushed to Bigtable when the
value used in the filter has a type Long, String, or Byte array (e.g.,
`rowKey < "some-value"`). Support for other types (e.g., Integer, Float, etc.)
will be added in the future.

| Filter               | Push down filter supported |
|----------------------|----------------------------|
| `EqualTo`            | Yes                        |
| `LessThan`           | Yes                        |
| `GreaterThan`        | Yes                        |
| `LessThanOrEqual`    | Yes                        |
| `GreaterThanOrEqual` | Yes                        |
| `StringStartsWith`   | Yes                        |
| `Or`                 | Yes                        |
| `And`                | Yes                        |
| `Not`                | No                         |
| Compound Row Key     | No                         |

When using compound row keys, filter on those columns are
**not** pushed to Bigtable and are performed on the client-side (resulting in a
full-table scan). If filtering is required, a workaround is to concatenate
the intended columns into a *single* DataFrame column of a supported type
(e.g., string) and use that column as the row key with one of the supported
filters above. One option is using the `concat` function, with a sample snippet
in Scala as follows:

```scala
df
  .withColumn(
    "new_row_key",
    org.apache.spark.sql.functions.concat(
      df.col("first_col"),
      df.col("second_col")
    )
  )
  .drop("first_col")
  .drop("second_col")
```

### Client-side metrics

Since the Bigtable Spark connector is based on the
[Bigtable Client for Java](https://github.com/googleapis/java-bigtable),
client-side metrics are enabled
inside the connector by default. You can refer to the
[client-side metrics](https://cloud.google.com/bigtable/docs/client-side-metrics)
documentation to find more details on accessing and interpreting these metrics.

### Use with Data Boost (Preview)

For read-only jobs, you can use Data Boost (Preview) serverless compute, a new
compute option for Bigtable that is specially optimized for high-throughput
pipeline job performance and production app serving traffic isolation
requirements. To use Data Boost,
you must create a Data Boost app profile and then provide the app profile ID for
the `spark.bigtable.app_profile.id` Spark option when you add your Bigtable
configuration to your Spark application. You can also convert an existing app
profile to a Data Boost app profile or specify a standard app profile to use
your instance's cluster nodes. You can refer to documentations on
[Data Boost](https://cloud.google.com/bigtable/docs/data-boost-overview) and
[App profiles](https://cloud.google.com/bigtable/docs/configuring-app-profiles)
for more information.

### Use low-level RDD functions with Bigtable

Since the Bigtable Spark connector is based on the
[Bigtable client for Java](https://github.com/googleapis/java-bigtable),
you can directly use the client in your Spark applications and perform
distributed read or write requests
within the low-level RDD functions such as `mapPartitions`
and `foreachPartition`.

To use the Bigtable client for Java classes, append the
`com.google.cloud.spark.bigtable.repackaged` prefix to the package names. For
example, instead of using the class name
as `com.google.cloud.bigtable.data.v2.BigtableDataClient`, use
`com.google.cloud.spark.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataClient`.

## Examples

You can access examples for Java, Scala, and Python inside the `examples`
directory. Each directory contains a `README.md` file with instruction on
running the example inside.
