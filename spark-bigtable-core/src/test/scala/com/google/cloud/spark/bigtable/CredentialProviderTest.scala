package com.google.cloud.spark.bigtable

import com.google.auth.Credentials
import com.google.auth.oauth2.GoogleCredentials.Builder
import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import com.google.cloud.spark.bigtable.auth.SparkBigtableCredentialsProvider

import org.scalatest.funsuite.AnyFunSuite

import java.util.Date

class CustomCredentialProvider extends SparkBigtableCredentialsProvider with Serializable {
  val expirationTime = new Date(System.currentTimeMillis() + 10000)
  val testToken = new AccessToken("test-initial-token", expirationTime)
  val credentialBuilder: Builder = new Builder().setAccessToken(testToken)
  override def getCredentials: Credentials = new GoogleCredentials(credentialBuilder) {
    override def refreshAccessToken(): AccessToken =
      new AccessToken("refreshed-token", new Date(System.currentTimeMillis() + 20000))
  }
}

//TODO : Tests to be added
class CredentialProviderTest extends AnyFunSuite {}
