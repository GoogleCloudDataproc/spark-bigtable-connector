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
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This class gives control over exactly what to return for each overridden API.
public class FakeCustomDataService extends BigtableGrpc.BigtableImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(FakeCustomDataService.class);

  private final List<SampleRowKeysResponse> sampleRowKeyResponses = new ArrayList<>();

  private final ConcurrentHashMap<String, List<ReadRowsResponse.CellChunk>> fakeTable =
      new ConcurrentHashMap<>();

  public void addSampleRowKeyResponse(SampleRowKeysResponse sampleRowKeysResponse) {
    sampleRowKeyResponses.add(sampleRowKeysResponse);
  }

  @Override
  public void sampleRowKeys(
      SampleRowKeysRequest request, StreamObserver<SampleRowKeysResponse> responseObserver) {
    try {
      for (SampleRowKeysResponse response : sampleRowKeyResponses) {
        responseObserver.onNext(response);
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.warn("Could not handle request " + request);
      throw new RuntimeException(e);
    }
  }

  public void addRow(String rowKey, String familyName, String qualifier, String value) {
    ReadRowsResponse.CellChunk chunk =
        ReadRowsResponse.CellChunk.newBuilder()
            .setRowKey(ByteString.copyFromUtf8(rowKey))
            .setFamilyName(StringValue.of(familyName))
            .setQualifier(BytesValue.of(ByteString.copyFromUtf8(qualifier)))
            .setValue(ByteString.copyFromUtf8(value))
            .setCommitRow(true)
            .build();

    List<ReadRowsResponse.CellChunk> rowChunks;
    if (fakeTable.containsKey(rowKey)) {
      rowChunks = fakeTable.get(rowKey);
      rowChunks.set(
          rowChunks.size() - 1,
          rowChunks.get(rowChunks.size() - 1).toBuilder().setCommitRow(false).build());
    } else {
      rowChunks = new ArrayList<>();
      fakeTable.put(rowKey, rowChunks);
    }

    rowChunks.add(chunk);
  }

  @Override
  public void readRows(ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
    // Extract requested row keys from ReadRowsRequest
    Set<String> requestedKeys = new HashSet<>();

    for (RowRange range : request.getRows().getRowRangesList()) {
      for (String key : fakeTable.keySet()) {
        if ((key.compareTo(range.getStartKeyOpen().toStringUtf8()) >= 0
                || range.getStartKeyOpen().isEmpty())
            && (key.compareTo(range.getEndKeyOpen().toStringUtf8()) < 0
                || range.getEndKeyOpen().isEmpty())) {
          requestedKeys.add(key);
        }
      }
    }
    for (ByteString rowKey : request.getRows().getRowKeysList()) {
      requestedKeys.add(rowKey.toStringUtf8());
    }

    List<String> sortedKeys = new ArrayList<>(requestedKeys);
    Collections.sort(sortedKeys);

    for (String rowKey : sortedKeys) {
      ReadRowsResponse.Builder responseBuilder = ReadRowsResponse.newBuilder();
      for (ReadRowsResponse.CellChunk chunk : fakeTable.get(rowKey)) {
        responseBuilder.addChunks(chunk);
      }
      responseObserver.onNext(responseBuilder.build());
    }
    responseObserver.onCompleted();
  }
}