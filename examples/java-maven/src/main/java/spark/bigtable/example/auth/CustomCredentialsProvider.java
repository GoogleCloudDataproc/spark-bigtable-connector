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

import com.google.auth.Credentials
import com.google.auth.oauth2.{AccessToken,GoogleCredentials}
import com.google.cloud.spark.bigtable.auth.SparkBigtableCredentialsProvider
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util.Date

public class CustomCredentialProvider implements SparkBigtableCredentialsProvider {
  Date expirationTime = new Date(System.currentTimeMillis() + 60000)
  AccessToken testToken = new AccessToken("test-initial-token", expirationTime)

    @Override
    Credentials getCredentials() {
      return GoogleCredentials.newBuilder()
        .setAccessToken(testToken)
        .setRefreshMargin(Duration.ofSeconds(2))
        .setExpirationMargin(Duration.ofSeconds(5))
        .build()
    }
}