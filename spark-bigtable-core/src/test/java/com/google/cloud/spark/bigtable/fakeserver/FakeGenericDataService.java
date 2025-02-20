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

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

// Overrides all the methods similarly, we can only customize whether to return an error or not.
public class FakeGenericDataService extends BigtableGrpc.BigtableImplBase {
  private final FakeServerCommon common = new FakeServerCommon();

  public void setNumOfResponses(int numOfResponses) {
    common.setNumOfResponses(numOfResponses);
  }

  public <T> void addError(T type, Status error) throws InterruptedException {
    common.addError(type, error);
  }

  @Override
  public void readRows(ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
    common.handleRequest(request, responseObserver, ReadRowsResponse.newBuilder().build());
  }

  @Override
  public void sampleRowKeys(
      SampleRowKeysRequest request, StreamObserver<SampleRowKeysResponse> responseObserver) {
    common.handleRequest(request, responseObserver, SampleRowKeysResponse.newBuilder().build());
  }

  @Override
  public void mutateRows(
      MutateRowsRequest request, StreamObserver<MutateRowsResponse> responseObserver) {
    common.handleRequest(request, responseObserver, MutateRowsResponse.newBuilder().build());
  }
}
