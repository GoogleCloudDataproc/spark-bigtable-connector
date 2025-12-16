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
   at `gs://spark-lib/bigtable/spark-bigtable_2.13-<version>.jar` or
   `gs://spark-lib/bigtable/spark-bigtable_2.12-<version>.jar`.

In Java and Scala applications, you can use different dependency management
tools (e.g., Maven, sbt, or Gradle) to access the
connector `com.google.cloud.spark.bigtable:spark-bigtable_2.13:<version>` or
`com.google.cloud.spark.bigtable:spark-bigtable_2.12:<version>` (current
`<version>` is `0.8.0`) and package it inside your application JAR
using libraries such as Maven Shade Plugin. For PySpark applications, you can
use the `--jars` flag to pass the GCS address of the connector when submitting
it.

For Maven, you can add the following snippet to your `pom.xml` file:

```xml
<!-- If you are using scala 2.13 -->
<dependency>
    <groupId>com.google.cloud.spark.bigtable</groupId>
    <artifactId>spark-bigtable_2.13</artifactId>
    <version>0.8.0</version>
</dependency>
```

```xml
<!-- If you are using scala 2.12 -->
<dependency>
    <groupId>com.google.cloud.spark.bigtable</groupId>
    <artifactId>spark-bigtable_2.12</artifactId>
    <version>0.8.0</version>
</dependency>
```

For sbt, you can add the following to your `build.sbt` file:

```
// for scala 2.13
libraryDependencies += "com.google.cloud.spark.bigtable" % "spark-bigtable_2.13" % "0.8.0"
```

```
// for scala 2.12
libraryDependencies += "com.google.cloud.spark.bigtable" % "spark-bigtable_2.12" % "0.8.0"
```

Finally, you can add the following to your `build.gradle` file when using
Gradle:

```
// for scala 2.13
dependencies {
implementation group: 'com.google.cloud.bigtable', name: 'spark-bigtable_2.13', version: '0.8.0'
}
```

```
// for scala 2.12
dependencies {
implementation group: 'com.google.cloud.bigtable', name: 'spark-bigtable_2.12', version: '0.8.0'
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

The connector supports the following Spark versions:

| Scala version | Spark versions                    | Spark Application Languages                             |
|---------------|-----------------------------------|---------------------------------------------------------|
| 2.13          | 3.1.x, 3.2.x, 3.4.x, 3.5.x        | Java, Scala, PySpark (`.py` files or Jupyter notebooks) |
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
which are created by concatenating multiple DataFrame columns together. When
using compound row keys keep the following restrictions in mind:

- A binary field of variable length may only occur as the last field in the
  row key
- Values for a string field of variable length that are part of a compound row
  key must always terminate on a unicode null character byte (`U+0000`), and
  when reading a row key the presence of this byte will be considered the end
  of that string value.

#### Catalog with variable column definitions

You can also use `regexColumns` to match multiple columns in the same column
family to a single data frame column. This can be useful in scenarios where
you don't know the exact column qualifiers for your data ahead of time, like
when your column qualifier is partially composed of other pieces of data.

For example this catalog:

```
{
  "table": {"name": "t1"},
  "rowkey": "id_rowkey",
  "columns": {
    "id": {"cf": "rowkey", "col": "id_rowkey", "type": "string"},
  },
  "regexColumns": {
    "metadata": {"cf": "info", "pattern": "\\C*", "type": "long" }
  }
}
```

Would match all columns in the column family "info" and the result would be a
DataFrame column named "metadata", where it's contents would be a Map of String
to Long with the keys being the column qualifiers and the values are the results
in those columns in Bigtable.

A few caveats:

- The values of all matching columns must be deserializable to the type defined
  in the catalog. If you expect to need more complex deserialization you can
  also define the type as `bytes` and run custom deserialization logic.
- A catalog with regex columns cannot be used for writes.
- Bigtable uses [RE2](https://github.com/google/re2/wiki/Syntax) for it's regex
  implementation, which has slight differences from other implementations.
- Because columns may contain arbitrary characters, including new lines, it is
  advisable to use `\C` as the wildcard expression, since `.` will not match on
  those
- Control characters must be escaped

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
  .option("catalog",catalog)
  .option("spark.bigtable.project.id",projectId)
  .option("spark.bigtable.instance.id",instanceId);
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

### Reading from Bigtable with complex Filters

You can read from Bigtable with any supported [filters](https://docs.cloud.google.com/bigtable/docs/using-filters) with
the `spark.bigtable.read.row.filters` option. This option expects a string which is the Base64 encoding of a
[Bigtable RowFilter](https://github.com/googleapis/java-bigtable/blob/v2.70.0/proto-google-cloud-bigtable-v2/src/main/java/com/google/bigtable/v2/RowFilter.java)
object.

```scala
import com.google.cloud.spark.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Filters.FILTERS
import com.google.cloud.spark.bigtable.repackaged.com.google.common.io.BaseEncoding

val filters = FILTERS.chain()
        .filter(FILTERS.family().exactMatch("info"))
        .filter(FILTERS.qualifier().regex("\\C*"))
val filterString = BaseEncoding.base64().encode(filters.toProto.toByteArray)

val dataFrame = spark
  .read()
  .format("bigtable")
  .option("catalog", catalog)
  .option("spark.bigtable.project.id", projectId)
  .option("spark.bigtable.instance.id", instanceId)
  .option("spark.bigtable.read.row.filters", filterString)
  .load();
```

### Efficient joins with other data sources

If you have a large DataFrame that you want to join with some Bigtable data and
you don't want to perform a full table scan, you can use `.joinWithBigtable` to
fetch data from Bigtable using one of your DataFrame's columns as a row key
filter for your Bigtable data.

This method will perform reads on Bigtable on each of the source DataFrame's
partitions separately, without the need to first collect the column containing
your key, as long as your column is of the same type as the `rowkey` column on
your Bigtable catalog.

Example:

```scala
// This import provides syntatic sugar for joinWithBigtable
import com.google.cloud.spark.bigtable.join.BigtableJoinImplicit._

// SrcDf is a large dataframe from some other source that we want to join with
// Bigtable. Let's say the column containing the bigtable key to join with is
// called `product_id`
val srcDf = ...;

// Reader options will be passed on to the reader, refer to the section "Reading
// from Bigtable" for more information
val readerOptions = Map(
  "spark.bigtable.project.id" -> projectId, // Required
  "spark.bigtable.instance.id" -> instanceId, // Required
  "catalog" -> catalog, // Required
)

val joinedDf = srcDf.joinWithBigtable(joinConfig, "product_id")
```

Or on pyspark:

```python
# This links to the JVM's method in spark
bigtable_join_class = spark._jvm.com.google.cloud.spark.bigtable.join.BigtableJoin

# Some other DataFrame
srcDf = ...

config_map = spark._jvm.java.util.HashMap()
config_map.put("spark.bigtable.project.id", bigtable_project_id)
config_map.put("spark.bigtable.instance.id", bigtable_instance_id)
config_map.put("catalog", catalog)

result_df = bigtable_join_class.joinWithBigtable(srcDf._jdf, config_map, "product_id", spark=spark._jsparkSession)
pyspark_result_df = DataFrame(result_df, spark)
```

A few important notes:

 - The key column on the source DataFrame must be of the same type as the row
   key column configured in the Bigtable catalog
 - Each partition on the source Dataframe will be fetched separately from
   Bigtable. This means you ideally want your source DataFrame to be sorted by
   the column containing the row key for more efficient Bigtable reads.

### Runtime configurations

You can use `.option(<config_name>, <config_value>)` in Spark to pass different
runtime configurations to
the connector. For example, Bigtable project and instance ID or settings for
timestamp and timeout configurations.
For a full list of configurations, refer to
[BigtableSparkConf.scala](spark-bigtable_2.12/src/main/scala/com/google/cloud/spark/bigtable/datasources/BigtableSparkConf.scala),
where these configs are defined.

### Custom authentication

If you are running Spark outside of Google Compute Engine (GCE) or Dataproc and rely on an internal service to provide a
Google AccessToken, you can implement custom authentication using the CredentialsProvider interface provided by the
Spark Bigtable connector.

To do this, implement the `com.google.cloud.spark.bigtable.repackaged.com.google.api.gax.core.CredentialsProvider`
interface. Your implementation should return a custom Credentials instance that encapsulates the AccessToken provided by
your internal service.

For example, create a class like CustomCredentialProvider:

```java
import com.google.cloud.spark.bigtable.repackaged.com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.spark.bigtable.repackaged.com.google.auth.Credentials;

import java.io.IOException;

public class CustomCredentialProvider implements CredentialsProvider, java.io.Serializable {
  @Override
  public Credentials getCredentials() throws IOException {
    return new CustomGoogleCredentials(); // This should internally return your AccessToken
  }
}

import com.google.cloud.spark.bigtable.repackaged.com.google.auth.oauth2.AccessToken;
import com.google.cloud.spark.bigtable.repackaged.com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;

public class CustomGoogleCredentials extends GoogleCredentials {
  private final GoogleCredentials credentials;
  private String currentToken;
  private Instant tokenExpiry;

  public CustomGoogleCredentials() throws IOException {
    this.credentials = GoogleCredentials.getApplicationDefault();
    this.currentToken = fetchInitialToken();
    this.tokenExpiry = fetchInitialTokenExpiry();
  }

  @Override
  public AccessToken refreshAccessToken() throws IOException {
    credentials.refresh();
    currentToken = credentials.getAccessToken().getTokenValue();
    tokenExpiry = credentials.getAccessToken().getExpirationTime().toInstant();

    if (isTokenExpired()) {
      credentials.refresh();
      currentToken = credentials.getAccessToken().getTokenValue();
      tokenExpiry = credentials.getAccessToken().getExpirationTime().toInstant();
    }

    return new AccessToken(currentToken, Date.from(tokenExpiry));
  }

  private boolean isTokenExpired() {
    return Instant.now().isAfter(tokenExpiry);
  }

  private String fetchInitialToken() throws IOException {
    credentials.refreshIfExpired();
    return credentials.getAccessToken().getTokenValue();
  }

  private Instant fetchInitialTokenExpiry() throws IOException {
    credentials.refreshIfExpired();
    return credentials.getAccessToken().getExpirationTime().toInstant();
  }
}
```

Your CustomCredentialsProvider class must extend
`com.google.cloud.spark.bigtable.repackaged.com.google.auth.CredentialsProvider`
and should handle returning the access
token and its expiration time.

Your implementation may optionally implement a constructor with a single
Map[String, String] parameter, and parameters can be supplied with options
prefixed with `spark.bigtable.auth.credentials_provider.args.`. These options
will be passed to your class constructor on initialization. For example:

```scala
class CustomAuthProvider(val params: Map[String, String]) extends CredentialsProvider {
  val param1 = params.get("spark.bigtable.auth.credentials_provider.args.param1")
  val anotherParam = params.get("spark.bigtable.auth.credentials_provider.args.another_param")

  private val proxyProvider = NoCredentialsProvider.create()

  override def getCredentials: Credentials = proxyProvider.getCredentials
}

// And when reading
val readDf = spark.read
  .format("bigtable")
  .option("catalog", Util.getCatalog(tableName))
  .option("spark.bigtable.project.id", projectId)
  .option("spark.bigtable.instance.id", instanceId)
  .option("spark.bigtable.auth.credentials_provider", "spark.bigtable.example.auth.CustomAuthProvider")
  .option("spark.bigtable.auth.credentials_provider.args.param1", "some-value")
  .option("spark.bigtable.auth.credentials_provider.args.another_param", "some-other-value")
```

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
| `In`                 | Yes                        |
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

You can use the Bigtable Spark connector to write and read low-level RDDs to
and from Bigtable. The connector manages steps such as connection
creation and caching to simplify the usage, while giving you freedom for how
exactly to convert between your RDD and Bigtable table (e.g., type conversion,
custom timestamps for each row, number of columns in each row, etc.). You can
create a new `BigtableRDD` object and call these functions for read and write
operations (note that this feature is only supported in Java and Scala,
not PySpark):

#### Writing an RDD

For writing, you need to pass in an RDD of
[RowMutationEntry](https://cloud.google.com/java/docs/reference/google-cloud-bigtable/latest/com.google.cloud.bigtable.data.v2.models.RowMutationEntry)
objects to the following function:

```scala
bigtableRDD.writeRDD(
  rdd: RDD[RowMutationEntry],
  tableId: String,
  bigtableSparkConf: BigtableSparkConf
)
```

#### Reading an RDD

When reading an RDD, you receive an RDD of Bigtable
[Row](https://cloud.google.com/java/docs/reference/google-cloud-bigtable/latest/com.google.cloud.bigtable.data.v2.models.Row)
objects after calling the following function:

```scala
BigtableRDD.readRDD(tableId: String, bigtableSparkConf: BigtableSparkConf)
```

Note that in both cases, you need to pass in a `BigtableSparkConf` object
corresponding to the options you want to set in your workflow. You can use the
`BigtableSparkConfBuilder` class to create an instance of this class:

```scala
val bigtableSparkConf: BigtableSparkConf =
  new BigtableSparkConfBuilder()
    .setProjectId(someProjectId)
    .setInstanceId(someInstanceId)
    .build()
```

A list of the setter methods for the supported configs is as follows:

1. `setProjectId(value: String)`
2. `setInstanceId(value: String)`
3. `setAppProfileId(value: String)`
4. `setReadRowsAttemptTimeoutMs(value: String)`
5. `setReadRowsTotalTimeoutMs(value: String)`
6. `setMutateRowsAttemptTimeoutMs(value: String)`
7. `setMutateRowsTotalTimeoutMs(value: String)`
8. `setBatchMutateSize(value: Int)`
9. `setEnableBatchMutateFlowControl(value: Boolean)`

You can refer to the
[official documentation](https://cloud.google.com/bigtable/docs/use-bigtable-spark-connector)
for more details about each of these options.

### Accessing Bigtable Java Client methods

Since the Bigtable Spark connector is based on the
[Bigtable client for Java](https://github.com/googleapis/java-bigtable),
you can directly use the client in your Spark applications, if you want
to have even more control over how you interact with Bigtable.

To use the Bigtable client for Java classes, append the
`com.google.cloud.spark.bigtable.repackaged` prefix to the package names. For
example, instead of using the class name
as `com.google.cloud.bigtable.data.v2.BigtableDataClient`, use
`com.google.cloud.spark.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataClient`.

## Examples

You can access examples for Java, Scala, and Python inside the `examples`
directory. Each directory contains a `README.md` file with instruction on
running the example inside.

Test
