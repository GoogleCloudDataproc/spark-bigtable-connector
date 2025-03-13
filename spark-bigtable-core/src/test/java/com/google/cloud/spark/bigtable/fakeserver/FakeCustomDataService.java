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

import com.google.bigtable.v2.*;
import com.google.common.collect.Queues;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This class gives control over exactly what to return for each overridden API.
public class FakeCustomDataService extends BigtableGrpc.BigtableImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(FakeCustomDataService.class);
    private final BlockingQueue<SampleRowKeysResponse> sampleRowKeyResponses =
            Queues.newLinkedBlockingDeque();

    private final BlockingQueue<ReadRowsResponse> readRowsResponses =
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

    private final ConcurrentHashMap<String, List<ReadRowsResponse.CellChunk>> fakeTable = new ConcurrentHashMap<>();

    public void addRow(String rowKey, String familyName, String qualifier, String value) {
        ReadRowsResponse.CellChunk chunk = ReadRowsResponse.CellChunk.newBuilder()
                .setRowKey(ByteString.copyFromUtf8(rowKey))
                .setFamilyName(StringValue.of(familyName))
                .setQualifier(BytesValue.of(ByteString.copyFromUtf8(qualifier)))
                .setValue(ByteString.copyFromUtf8(value))
                .setCommitRow(true)
                .build();

        fakeTable.computeIfAbsent(rowKey, k -> new ArrayList<>()).add(chunk);
    }

    @Override
    public void readRows(ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
        // Extract requested row keys from ReadRowsRequest
        Set<String> requestedKeys = new HashSet<>();
        for (RowRange range : request.getRows().getRowRangesList()) {
            for (String key : fakeTable.keySet()) {
                if (key.compareTo(range.getStartKeyOpen().toStringUtf8()) >= 0 &&
                        key.compareTo(range.getEndKeyOpen().toStringUtf8()) < 0) {
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
