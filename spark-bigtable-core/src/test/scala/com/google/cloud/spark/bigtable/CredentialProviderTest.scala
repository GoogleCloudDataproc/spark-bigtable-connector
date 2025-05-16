package com.google.cloud.spark.bigtable

import com.google.api.gax.core.CredentialsProvider
import com.google.auth.Credentials
import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util.Date

class CustomCredentialProvider extends CredentialsProvider with Serializable {
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
