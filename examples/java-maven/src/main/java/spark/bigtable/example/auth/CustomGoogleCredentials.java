package spark.bigtable.example.auth;

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

import com.google.cloud.spark.bigtable.repackaged.com.google.auth.oauth2.AccessToken;
import com.google.cloud.spark.bigtable.repackaged.com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;

public class CustomGoogleCredentials extends GoogleCredentials {
  private final GoogleCredentials credentials;
  private String currentToken;
  private Instant tokenExpiry;

  public CustomGoogleCredentials() throws IOException {
    this.credentials = GoogleCredentials.getApplicationDefault();
    this.currentToken = fetchInitialToken();
    this.tokenExpiry = fetchInitialTokenExpiry();
  }

  @Override
  public AccessToken refreshAccessToken() throws IOException {
    credentials.refresh();
    currentToken = credentials.getAccessToken().getTokenValue();
    tokenExpiry = credentials.getAccessToken().getExpirationTime().toInstant();

    if (isTokenExpired()) {
      credentials.refresh();
      currentToken = credentials.getAccessToken().getTokenValue();
      tokenExpiry = credentials.getAccessToken().getExpirationTime().toInstant();
    }

    return new AccessToken(currentToken, Date.from(tokenExpiry));
  }

  private boolean isTokenExpired() {
    return Instant.now().isAfter(tokenExpiry);
  }

  private String fetchInitialToken() throws IOException {
    credentials.refreshIfExpired();
    return credentials.getAccessToken().getTokenValue();
  }

  private Instant fetchInitialTokenExpiry() throws IOException {
    credentials.refreshIfExpired();
    return credentials.getAccessToken().getExpirationTime().toInstant();
  }
}