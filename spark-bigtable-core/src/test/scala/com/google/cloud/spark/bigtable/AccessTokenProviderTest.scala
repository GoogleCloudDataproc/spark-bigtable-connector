package com.google.cloud.spark.bigtable

import org.scalatest.funsuite.AnyFunSuite
import java.util.Date

class TestAccessTokenProvider extends AccessTokenProvider {
  override def getAccessToken(): AccessToken = {
    new AccessToken("test-token", new Date(System.currentTimeMillis() + 10000))
  }
}

class AccessTokenProviderTest extends AnyFunSuite {

  test("AccessTokenProvider returns a valid access token") {
    val provider: AccessTokenProvider = new TestAccessTokenProvider
    val token: AccessToken = provider.getAccessToken()

    assert(token != null, "Token should not be null")
    assert(token.getTokenValue == "test-token", "Token value should be 'test-token'")
    assert(token.getExpirationTime.after(new Date()), "Expiration time should be in the future")
  }
}
