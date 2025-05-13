package com.google.cloud.spark.bigtable

import com.google.auth.Credentials
import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import com.google.cloud.spark.bigtable.auth.SparkBigtableCredentialsProvider
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util.Date

class CustomCredentialProvider extends SparkBigtableCredentialsProvider with Serializable {
  val expirationTime = new Date(System.currentTimeMillis() + 60000)
  val testToken = new AccessToken("test-initial-token", expirationTime)

  override def getCredentials: Credentials = GoogleCredentials.newBuilder()
    .setAccessToken(testToken)
    .setRefreshMargin(Duration.ofSeconds(2))
    .setExpirationMargin(Duration.ofSeconds(5))
    .build()
}

//TODO : Tests to be added
class CredentialProviderTest extends AnyFunSuite {}
