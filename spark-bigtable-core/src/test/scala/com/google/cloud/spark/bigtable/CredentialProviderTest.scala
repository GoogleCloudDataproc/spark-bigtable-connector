package com.google.cloud.spark.bigtable

import com.google.api.gax.core.CredentialsProvider
import com.google.auth.Credentials
import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import com.google.cloud.spark.bigtable.fakeserver.{
  FakeCustomDataService,
  FakeGenericDataService,
  FakeServerBuilder
}
import com.google.cloud.spark.bigtable.util.Reflector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util.Date

class CustomCredentialProvider extends CredentialsProvider with Serializable {
  val expirationTime = new Date(System.currentTimeMillis() + 60000)
  val testToken = new AccessToken("test-initial-token", expirationTime)

  override def getCredentials: Credentials = GoogleCredentials
    .newBuilder()
    .setAccessToken(testToken)
    .setRefreshMargin(Duration.ofSeconds(2))
    .setExpirationMargin(Duration.ofSeconds(5))
    .build()
}

class CredentialProviderTest
    extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Logging {

  @transient lazy implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("BigtableJoinTest")
    .master("local[*]")
    .getOrCreate()

  val customCredentialProviderFQCN: String = new CustomCredentialProvider().getClass.getName

  val basicCatalog: String =
    s"""{
       |"table":{"name":"tableName"},
       |"rowkey":"stringCol",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"stringCol", "type":"string"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"string"}
       |}
       |}""".stripMargin
  var fakeCustomDataService: FakeCustomDataService = _
  var fakeGenericDataService: FakeGenericDataService = _
  var emulatorPort: String = _

  override def beforeAll(): Unit = {}

  override def afterAll(): Unit = {
    spark.stop()
  }

  override def beforeEach(): Unit = {
    fakeCustomDataService = new FakeCustomDataService
    val server = new FakeServerBuilder()
      .addService(fakeCustomDataService)
      .start
    emulatorPort = Integer.toString(server.getPort)
    logInfo("Bigtable mock server started on port " + emulatorPort)
  }

  test("Instantiation of implementation of the CredentialsProvider") {
    val customCredentialProvider =
      Reflector.createVerifiedInstance(customCredentialProviderFQCN, classOf[CredentialsProvider])
    assert(customCredentialProvider.getCredentials.isInstanceOf[Credentials])
  }
}
