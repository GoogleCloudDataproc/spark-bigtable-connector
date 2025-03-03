package com.google.cloud.spark.bigtable

import org.scalatest.funsuite.AnyFunSuite
import java.util.Date

class TestAccessTokenProvider extends AccessTokenProvider {
  private var accessToken: AccessToken = new AccessToken("test-token", new Date(System.currentTimeMillis() + 10000))
  override def getAccessToken(): AccessToken = accessToken

  override def refresh(): Unit = {
    accessToken = new AccessToken("refreshed-token", new Date(System.currentTimeMillis() + 20000))
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

  test("AccessTokenProvider refresh updates the token") {
    val provider: AccessTokenProvider = new TestAccessTokenProvider
    val oldToken: AccessToken = provider.getAccessToken()

    provider.refresh()
    val newToken: AccessToken = provider.getAccessToken()

    assert(newToken.getTokenValue == "refreshed-token", "Token should be updated after refresh")
    assert(newToken.getExpirationTime.after(oldToken.getExpirationTime), "New token should have a later expiration")
  }
}
