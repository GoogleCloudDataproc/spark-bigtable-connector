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

import com.google.cloud.spark.bigtable.repackaged.com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.spark.bigtable.repackaged.com.google.auth.Credentials;

import java.io.IOException;

public class CustomCredentialProvider implements CredentialsProvider, java.io.Serializable {

  @Override
  public Credentials getCredentials() throws IOException {
    return new CustomGoogleCredentials();
  }
}
