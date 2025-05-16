/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.bigtable.example.auth

import com.google.cloud.spark.bigtable.repackaged.com.google.api.gax.core.`CredentialsProvider`
import com.google.cloud.spark.bigtable.repackaged.com.google.auth.Credentials
import com.google.cloud.spark.bigtable.repackaged.com.google.auth.oauth2.{AccessToken, GoogleCredentials}

import java.io.IOException
import java.time.Instant
import java.util.Date

class CustomCredentialProvider extends CredentialsProvider with Serializable {
  override def getCredentials: Credentials = new CustomGoogleCredentials
}

class CustomGoogleCredentials extends GoogleCredentials {
  private val credentials: GoogleCredentials = GoogleCredentials.getApplicationDefault
  private var currentToken: String = fetchInitialToken()
  private var tokenExpiry: Instant = fetchInitialTokenExpiry()


  @throws[IOException]
  override def refreshAccessToken: AccessToken = {
    credentials.refresh()
    currentToken = credentials.getAccessToken.getTokenValue
    tokenExpiry = credentials.getAccessToken.getExpirationTime.toInstant
    if (isTokenExpired) refresh()
    new AccessToken(currentToken, Date.from(tokenExpiry))
  }

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