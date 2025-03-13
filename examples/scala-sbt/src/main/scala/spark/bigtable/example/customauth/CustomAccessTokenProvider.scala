package spark.bigtable.example.customauth

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.spark.bigtable.customauth.{AccessToken, AccessTokenProvider}

import java.io.IOException
import java.time.Instant
import java.util.Date

class CustomAccessTokenProvider extends AccessTokenProvider {
  private val credentials: GoogleCredentials = GoogleCredentials.getApplicationDefault

  private var currentToken: String = fetchInitialToken()
  private var tokenExpiry: Instant = fetchInitialTokenExpiry()

  @throws(classOf[IOException])
  override def getAccessToken(): AccessToken = {
    if (isTokenExpired) {
      refresh()
    }
    new AccessToken(currentToken, Date.from(tokenExpiry))
  }

  @throws(classOf[IOException])
  override def refresh(): Unit = {
    credentials.refresh()
    currentToken = credentials.getAccessToken.getTokenValue
    tokenExpiry = credentials.getAccessToken.getExpirationTime.toInstant
  }

  @throws(classOf[IOException])
  def getCurrentToken: String = currentToken

  private def isTokenExpired: Boolean = {
    Instant.now().isAfter(tokenExpiry)
  }

  @throws(classOf[IOException])
  private def fetchInitialToken(): String = {
    credentials.refreshIfExpired()
    credentials.getAccessToken.getTokenValue
  }

  @throws(classOf[IOException])
  private def fetchInitialTokenExpiry(): Instant = {
    credentials.refreshIfExpired()
    credentials.getAccessToken.getExpirationTime.toInstant
  }
}