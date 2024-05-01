# Spark Bigtable Example Using Java and Maven

This example uses Apache Spark with Java and Maven for package management to
write data to a Bigtable table and read it back.

## Compiling the project

To compile the code, you can run
the following command (after installing Maven) from inside the current
directory:

```
mvn clean install
```

The target JAR will be located under
`target/spark-bigtable-example-0.0.1-SNAPSHOT.jar`.

## Running the example

To run the JAR, you will need a Bigtable project and instance ID, as well as a
Bigtable table name, which will be the three required arguments. By default, a
new table is created by the application, but you can provide an optional fourth
argument `false` for `createNewTable` to avoid doing so. (Assuming that you have
already created a table with the column family `example_family`.)

## Using Dataproc Cluster

Assuming you have already created a Dataproc cluster, you can
use the following command to submit the JAR to the cluster:

```shell
gcloud dataproc jobs submit spark \
--cluster=$BIGTABLE_SPARK_DATAPROC_CLUSTER \
--region=$BIGTABLE_SPARK_DATAPROC_REGION \
--class=spark.bigtable.example.WordCount \
--jars=target/spark-bigtable-example-0.0.1-SNAPSHOT.jar  \
--  \
$BIGTABLE_SPARK_PROJECT_ID \
$BIGTABLE_SPARK_INSTANCE_ID \
$BIGTABLE_SPARK_TABLE_NAME
```

## Using local Spark

You can run the following command to run the example locally,
assuming you have Spark installed on your machine:

```shell
spark-submit \
target/spark-bigtable-example-0.0.1-SNAPSHOT.jar \
$BIGTABLE_SPARK_PROJECT_ID \
$BIGTABLE_SPARK_INSTANCE_ID \
$BIGTABLE_SPARK_TABLE_NAME
```

## Expected output

The following text should be shown in the output of the Spark job.

```
Reading the DataFrame from Bigtable:
+-----+-----+---------------+
|count| word|frequencyDouble|
+-----+-----+---------------+
|    0|word0|            0.0|
|    1|word1|          0.001|
|    2|word2|          0.002|
|    3|word3|          0.003|
|    4|word4|          0.004|
|    5|word5|          0.005|
|    6|word6|          0.006|
|    7|word7|          0.007|
|    8|word8|          0.008|
|    9|word9|          0.009|
+-----+-----+---------------+
```

To verify that the data has been written to Bigtable, you can run the following
command (
requires [cbt CLI](https://cloud.google.com/bigtable/docs/cbt-overview)):

```
cbt -project=$BIGTABLE_SPARK_PROJECT_ID -instance=$BIGTABLE_SPARK_INSTANCE_ID \
read $BIGTABLE_SPARK_TABLE_NAME
```

With this expected output:

```
----------------------------------------
word0
  example_family:countCol                  @ 2024/04/30-14:54:16.401000
    "\x00\x00\x00\x00\x00\x00\x00\x00"
  example_family:frequencyCol              @ 2024/04/30-14:54:16.401000
    "\x00\x00\x00\x00\x00\x00\x00\x00"

----------------------------------------
word1
  example_family:countCol                  @ 2024/04/30-14:54:16.401000
    "\x00\x00\x00\x00\x00\x00\x00\x01"
  example_family:frequencyCol              @ 2024/04/30-14:54:16.401000
    "?PbM\xd2\xf1\xa9\xfc"

----------------------------------------
.
.
.
```