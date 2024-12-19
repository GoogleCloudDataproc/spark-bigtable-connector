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
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.common.collect.Queues;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This class gives control over exactly what to return for each overridden API.
public class FakeCustomDataService extends BigtableGrpc.BigtableImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(FakeCustomDataService.class);
  private final BlockingQueue<SampleRowKeysResponse> sampleRowKeyResponses =
      Queues.newLinkedBlockingDeque();

  public void addSampleRowKeyResponse(SampleRowKeysResponse sampleRowKeysResponse) {
    try {
      sampleRowKeyResponses.put(sampleRowKeysResponse);
    } catch (InterruptedException e) {
      LOG.warn("Could not add response " + sampleRowKeysResponse);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sampleRowKeys(
      SampleRowKeysRequest request, StreamObserver<SampleRowKeysResponse> responseObserver) {
    try {
      while (!sampleRowKeyResponses.isEmpty()) {
        responseObserver.onNext(sampleRowKeyResponses.take());
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.warn("Could not handle request " + request);
      throw new RuntimeException(e);
    }
  }
}
