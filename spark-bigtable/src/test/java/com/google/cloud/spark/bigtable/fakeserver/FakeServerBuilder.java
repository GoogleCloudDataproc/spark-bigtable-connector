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

package com.google.cloud.spark.bigtable.fakeserver;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

public class FakeServerBuilder {
  private List<BindableService> services = new ArrayList<>();

  public FakeServerBuilder addService(BindableService service) {
    services.add(service);
    return this;
  }

  private Server attemptOneStart() throws IOException {
    int port;
    try (ServerSocket ss = new ServerSocket(0)) {
      port = ss.getLocalPort();
    }
    ServerBuilder<?> builder = ServerBuilder.forPort(port);
    for (BindableService service : services) {
      builder.addService(service);
    }
    return builder.build().start();
  }

  public Server start() throws IOException {
    IOException lastError = null;
    int numOfRetries = 5;
    for (int i = 0; i < numOfRetries; i++) {
      try {
        return attemptOneStart();
      } catch (IOException e) {
        lastError = e;
      }
    }

    throw lastError;
  }
}
