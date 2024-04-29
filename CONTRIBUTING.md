# How to Contribute

We'd love to accept your patches and contributions to this project.

## Before you begin

### Sign our Contributor License Agreement

Contributions to this project must be accompanied by a
[Contributor License Agreement](https://cla.developers.google.com/about) (CLA).
You (or your employer) retain the copyright to your contribution; this simply
gives us permission to use and redistribute your contributions as part of the
project.

If you or your current employer have already signed the Google CLA (even if it
was for a different project), you probably don't need to do it again.

Visit <https://cla.developers.google.com/> to see your current agreements or to
sign a new one.

### Review our Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/).

## Contribution process

### Code Reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

### Development and Testing

You can use the following command to compile and install the project:
```shell
mvn clean install -DskipTests
```
The connector's jar file will be located at
`spark-bigtable_2.12/target/spark-bigtable_2.12-0.1.0.jar`.
(Note the use of `-DskipTests`, as otherwise all tests will be run,
which takes hours. This option still compiles the test JARs.)

To run the unit tests in the `spark-bigtable_2.12` module, you can use this command:
```shell
mvn -pl spark-bigtable test
```

To run the integration tests in the `spark-bigtable_2.12-it` module, you can use this command:

```shell
mvn -pl spark-bigtable_2.12-it failsafe:integration-test failsafe:verify -DbigtableProjectId=${BIGTABLE_PROJECT_ID} -DbigtableInstanceId=${BIGTABLE_INSTANCE_ID} -P integration
```

In the above command, you can replace `-P integration` with `-P long-running`
or `-P fuzz` to run the long-running and fuzz tests, respectively.
(or use `-P integration,long-running,fuzz` to run all
tests). However, note that by design, these tests take hours to run.

### Code formatting
We use [google-java-format](https://github.com/google/google-java-format) to
format Java code. To use this formatter, you can download the JAR and run this
command on a java file that you have udpated:
```shell
java -jar /path/to/google-java-format-${GJF_VERSION?}-all-deps.jar -i /path/to/java/source/code.java
```

We use [Scalafmt](https://scalameta.org/scalafmt/) for Scala code, with the
configurations defined in `.scalafmt.conf`. You can refer to the
[Scalafmt documentation](https://scalameta.org/scalafmt/docs/installation.html)
to enable it in your development environment.