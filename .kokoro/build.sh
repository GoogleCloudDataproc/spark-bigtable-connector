#!/bin/bash
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

## Get the directory of the build script
scriptDir=$(realpath $(dirname "${BASH_SOURCE[0]}"))
## cd to the parent directory, i.e. the root of the git repo
cd ${scriptDir}/..

# include common functions
source ${scriptDir}/common.sh

# Print out Maven & Java version
mvn -version
echo ${JOB_TYPE}

# attempt to install 3 times with exponential backoff (starting with 10 seconds)
retry_with_backoff 3 10 \
  mvn install -B -V -ntp \
    -DskipTests=true \
    -Dclirr.skip=true \
    -Denforcer.skip=true \
    -Dmaven.javadoc.skip=true \
    -Dgcloud.download.skip=true \
    -T 1C

# if GOOGLE_APPLICATION_CREDENTIALS is specified as a relative path, prepend Kokoro root directory onto it
if [[ ! -z "${GOOGLE_APPLICATION_CREDENTIALS}" && "${GOOGLE_APPLICATION_CREDENTIALS}" != /* ]]; then
    export GOOGLE_APPLICATION_CREDENTIALS=$(realpath ${KOKORO_GFILE_DIR}/${GOOGLE_APPLICATION_CREDENTIALS})
fi

RETURN_CODE=0
set +e

echo "Installing the cbt CLI command."
apt install -y google-cloud-sdk-cbt

run_unit_tests() {
    echo "***Running connector's unit tests.***"
    CONNECTOR_MODULE="spark-bigtable_2.12"
    mvn -pl ${CONNECTOR_MODULE} -am  \
        test -B -ntp -Dclirr.skip=true -Denforcer.skip=true -Dcheckstyle.skip
    return $?
}

run_bigtable_spark_tests() {
    SPARK_VERSION=$1
    MAVEN_PROFILES=$2
    echo "***Running Spark-Bigtable tests for Spark ${SPARK_VERSION} and profile(s) ${MAVEN_PROFILES}.***"
    BIGTABLE_SPARK_IT_MODULE="spark-bigtable_2.12-it"
    mvn -pl ${BIGTABLE_SPARK_IT_MODULE} \
        failsafe:integration-test failsafe:verify \
        -B -ntp -Dclirr.skip=true -Denforcer.skip=true -Dcheckstyle.skip \
        -Dspark.version=${SPARK_VERSION} \
        -DbigtableProjectId=${BIGTABLE_PROJECT_ID} \
        -DbigtableInstanceId=${BIGTABLE_INSTANCE_ID} \
        -P ${MAVEN_PROFILES}
    return $?
}

get_bigtable_spark_jar() {
    # This makes the script independent of the connector's version and
    # ignores the source code JAR.
    echo $(ls spark-bigtable_2.12/target/spark-bigtable_2.12-* | grep -v sources)
}

create_table_id() {
    TEST_TYPE=$1
    # Generate a random id with the 'cbt-TEST_TYPE-1682536895-138fa' format.
    echo "cbt-${TEST_TYPE}-$(date +%s)-$(echo $RANDOM | md5sum | head -c 5)"
}

# Create a table with pre-splits at random bytes. Some caveats:
#  1. character \054 (comma) gets ignored because ',' is also the delimiter. we
#      can probably avoid this with escaping, etc., but it's not really critical
#      since there's nothing special about its byte value to try to manually cover.
#  2. Repeated characters would be ignored w/o affecting correctness.
create_table_with_random_splits() {
    BIGTABLE_PROJECT_ID=$1
    BIGTABLE_INSTANCE_ID=$2
    TABLE_ID=$3

    num_of_splits=$(($RANDOM % 13 + 3))  # Random number of splits, between 3 and 15
    ascii_code_list=""
    split_points=""
    for _ in $(seq 1 $num_of_splits); do
        random_ascii_code=$(($RANDOM % 256))  # Range 0-255 for full ASCII
        ascii_code_list+="$random_ascii_code,"
        random_char=$(printf "\\$(printf '%03o' "$random_ascii_code")")
        if [[ -n $split_points ]]; then  # Add comma before, except for the first character
            split_points+=","
        fi
        split_points+="$random_char"
    done
    echo "Creating table $TABLE_ID with $num_of_splits split points $ascii_code_list"
    cbt -project="${BIGTABLE_PROJECT_ID}" -instance="${BIGTABLE_INSTANCE_ID}" \
            createtable "${TABLE_ID}" "families=col_family" splits="$split_points"
}

delete_table() {
    BIGTABLE_PROJECT_ID=$1
    BIGTABLE_INSTANCE_ID=$2
    TABLE_ID=$3
    cbt -project=${BIGTABLE_PROJECT_ID} -instance=${BIGTABLE_INSTANCE_ID} \
        deletetable  ${TABLE_ID}
    if [[ $? != 0 ]]; then echo "Could not delete table for PySpark test."; fi
}

run_load_test() {
    echo "***Running Load test.***"
    BIGTABLE_SPARK_JAR=$(get_bigtable_spark_jar)
    TEST_SCRIPT="spark-bigtable_2.12/test-pyspark/load_test.py"
    BASE_SCRIPT="spark-bigtable_2.12/test-pyspark/test_base.py"
    TABLE_ID=$(create_table_id "load")
    gcloud dataproc jobs submit pyspark \
        --cluster=${DATAPROC_CLUSTER_NAME} \
        --region=${DATAPROC_CLUSTER_REGION} \
        --jars=${BIGTABLE_SPARK_JAR} \
        ${TEST_SCRIPT} \
        --py-files=${BASE_SCRIPT} \
        -- \
        --bigtableProjectId=${BIGTABLE_PROJECT_ID} \
        --bigtableInstanceId=${BIGTABLE_INSTANCE_ID} \
        --bigtableTableId=${TABLE_ID}
    exit_code=$?
    delete_table ${BIGTABLE_PROJECT_ID} ${BIGTABLE_INSTANCE_ID} ${TABLE_ID}
    return $exit_code
}

run_fuzz_tests() {
    SPARK_VERSION=$1
    echo "***Running Spark-Bigtable fuzz tests for Spark ${SPARK_VERSION}.***"
    TABLE_ID=$(create_table_id "fuzz")
    create_table_with_random_splits \
      "${BIGTABLE_PROJECT_ID}" "${BIGTABLE_INSTANCE_ID}" "$TABLE_ID"
    BIGTABLE_SPARK_IT_MODULE="spark-bigtable_2.12-it"
    mvn -pl ${BIGTABLE_SPARK_IT_MODULE} \
        failsafe:integration-test failsafe:verify \
        -B -ntp -Dclirr.skip=true -Denforcer.skip=true -Dcheckstyle.skip \
        -Dspark.version="${SPARK_VERSION}" \
        -DbigtableProjectId="${BIGTABLE_PROJECT_ID}" \
        -DbigtableInstanceId="${BIGTABLE_INSTANCE_ID}" \
        -DbigtableTableId="${TABLE_ID}" \
        -P fuzz
    exit_code=$?
    delete_table "${BIGTABLE_PROJECT_ID}" "${BIGTABLE_INSTANCE_ID}" "${TABLE_ID}"
    return $exit_code
}

run_pyspark_test() {
    SPARK_VERSION=$1
    HADOOP_VERSION=$2
    # TODO: Install Python 3.7 on Kokoro machines to enable PySpark 2.4.8 tests
    #   without running into the "TypeError: an integer is required" issue.
    if [[ ${SPARK_VERSION} == "2.4.8" ]]; then return; fi

    echo "***Running the PySpark test for Spark ${SPARK_VERSION}.***"
    SPARK_BIN_NAME="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
    BIGTABLE_SPARK_JAR=$(get_bigtable_spark_jar)
    PYSPARK_TEST_SCRIPT="spark-bigtable_2.12/test-pyspark/read_and_write_test.py"
    BASE_SCRIPT="spark-bigtable_2.12/test-pyspark/test_base.py"
    # Download Apache Spark on the machine from a GCS bucket, new versions
    # must be manually uploaded to the bucket first.
    gsutil -q cp "gs://bigtable-spark-test-resources/spark-files/${SPARK_BIN_NAME}.tgz" .
    tar xzf ${SPARK_BIN_NAME}.tgz
    PYSPARK_TABLE_ID=$(create_table_id "pyspark")
    ./${SPARK_BIN_NAME}/bin/spark-submit \
        --packages=org.slf4j:slf4j-reload4j:1.7.36 \
        --jars ${BIGTABLE_SPARK_JAR} \
        --py-files=${BASE_SCRIPT} \
        ${PYSPARK_TEST_SCRIPT} \
        --bigtableProjectId=${BIGTABLE_PROJECT_ID} \
        --bigtableInstanceId=${BIGTABLE_INSTANCE_ID} \
        --bigtableTableId=${PYSPARK_TABLE_ID}
    exit_code=$?
    delete_table ${BIGTABLE_PROJECT_ID} ${BIGTABLE_INSTANCE_ID} ${PYSPARK_TABLE_ID}
    return $exit_code
}

case ${JOB_TYPE} in
presubmit)
    run_bigtable_spark_tests "3.1.3" "integration"
    RETURN_CODE=$?
    run_pyspark_test "3.1.3" "3.2"
    RETURN_CODE=$(($RETURN_CODE || $?))
    run_unit_tests
    RETURN_CODE=$(($RETURN_CODE || $?))
    ;;
all_versions)
    RETURN_CODE=0
    for SPARK_VERSION in "2.4.8" "3.1.3" "3.3.0" "3.4.2"
    do
        run_bigtable_spark_tests ${SPARK_VERSION} "integration"
        RETURN_CODE=$(($RETURN_CODE || $?))
    done
    for SPARK_HADOOP_VERSIONS in "2.4.8 2.7" "3.1.3 3.2" "3.3.0 3" "3.4.2 3"
    do
        run_pyspark_test ${SPARK_HADOOP_VERSIONS}
        RETURN_CODE=$(($RETURN_CODE || $?))
    done
    run_unit_tests
    RETURN_CODE=$(($RETURN_CODE || $?))
    ;;
fuzz)
    run_fuzz_tests "3.1.3"
    RETURN_CODE=$?
    ;;
long_running)
    run_bigtable_spark_tests "3.1.3" "long-running"
    RETURN_CODE=$?
    ;;
load)
    run_load_test
    RETURN_CODE=$?
    ;;
*)
    ;;
esac

# fix output location of logs
bash .kokoro/coerce_logs.sh

echo "exiting with ${RETURN_CODE}"
exit ${RETURN_CODE}
