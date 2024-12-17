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

import com.google.bigtable.admin.v2.BigtableTableAdminGrpc;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.admin.v2.internal.NameUtil;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class FakeTableAdminService extends BigtableTableAdminGrpc.BigtableTableAdminImplBase {
  private final FakeServerCommon common = new FakeServerCommon();

  public <T> void addError(T type, Status error) throws InterruptedException {
    common.addError(type, error);
  }

  @Override
  public void createTable(CreateTableRequest request, StreamObserver<Table> responseObserver) {
    Table resp =
        Table.newBuilder()
            .setName(NameUtil.formatTableName("fake-project", "fake-instance", "fake-table"))
            .build();
    common.handleRequest(request, responseObserver, resp);
  }
}
