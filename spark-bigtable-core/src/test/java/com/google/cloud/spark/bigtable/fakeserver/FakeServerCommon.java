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

import com.google.common.collect.Queues;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.LinkedHashMap;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FakeServerCommon {
  private static final Logger LOG = LoggerFactory.getLogger(FakeServerCommon.class);
  private final LinkedHashMap<Class<? extends Object>, BlockingQueue<Status>> errorsToThrow =
      new LinkedHashMap<Class<? extends Object>, BlockingQueue<Status>>();

  // To allow SampleRowKeys to return more than one tablet, so we can test a scenario where reading
  // only one partition fails.
  private int numOfResponses = 1;

  void setNumOfResponses(int numOfResponses) {
    this.numOfResponses = numOfResponses;
  }

  <T> void addError(T type, Status error) throws InterruptedException {
    try {
      Class<?> key = type.getClass();
      if (!errorsToThrow.containsKey(key)) {
        errorsToThrow.put(key, Queues.newLinkedBlockingDeque());
      }
      errorsToThrow.get(key).put(error);
    } catch (InterruptedException e) {
      LOG.warn("Could not add error to the server");
      throw e;
    }
  }

  <RequestT, ResponseT> void handleRequest(
      RequestT request, StreamObserver<ResponseT> responseObserver, ResponseT resp) {
    try {
      Class<? extends Object> key = request.getClass();
      if (errorsToThrow.containsKey(key) && !errorsToThrow.get(key).isEmpty()) {
        responseObserver.onError(new StatusRuntimeException(errorsToThrow.get(key).take()));
      } else {
        for (int i = 0; i < numOfResponses; i++) {
          responseObserver.onNext(resp);
        }
        responseObserver.onCompleted();
      }
    } catch (Exception e) {
      LOG.warn("Could not handle request of type " + request.getClass().getName());
      throw new RuntimeException(e);
    }
  }
}
