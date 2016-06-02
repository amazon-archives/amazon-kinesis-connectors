/*
 * Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.connectors.impl;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;

import java.util.LinkedList;
import java.util.List;

/**
 * ConstantRecordCountMemoryBuffer is an implementation of the IBuffer interface.
 * It ensures that records are periodically flushed with the same record count.
 *
 * Flushing a constant number of records is essential to the proper handling of
 * duplicates on the consumer end as documented here:
 *
 * http://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-duplicates.html
 *
 * @param <T>
 */
public class ConstantRecordCountMemoryBuffer<T> implements IBuffer<T> {

    private final KinesisConnectorConfiguration configuration;
    private final List<BasicMemoryBuffer<T>> buffers;
    private BasicMemoryBuffer<T> currentBuffer;

    public ConstantRecordCountMemoryBuffer(KinesisConnectorConfiguration configuration, List<BasicMemoryBuffer<T>> buffers) {
        this.configuration = configuration;
        this.buffers = buffers;
        if (this.buffers.isEmpty()) {
            currentBuffer = new BasicMemoryBuffer<T>(configuration);
            buffers.add(currentBuffer);
        }
    }

    public ConstantRecordCountMemoryBuffer(KinesisConnectorConfiguration configuration) {
        this(configuration, new LinkedList<BasicMemoryBuffer<T>>());
    }

    @Override
    public long getBytesToBuffer() {
        return configuration.BUFFER_BYTE_SIZE_LIMIT;
    }

    @Override
    public long getNumRecordsToBuffer() {
        return configuration.BUFFER_RECORD_COUNT_LIMIT;
    }

    @Override
    public long getMillisecondsToBuffer() {
        return configuration.BUFFER_MILLISECONDS_LIMIT;
    }

    @Override
    public void consumeRecord(T record, int recordSize, String sequenceNumber) {
        assert(!buffers.isEmpty());

        if (currentBuffer.getRecords().size() >= getNumRecordsToBuffer()) {
            currentBuffer = new BasicMemoryBuffer<>(configuration);
            buffers.add(currentBuffer);
        }

        currentBuffer.consumeRecord(record, recordSize, sequenceNumber);
    }

    @Override
    public void clear() {
        assert(!buffers.isEmpty());

        if (buffers.size() == 1) {
            currentBuffer.clear();
        } else {
           buffers.remove(0);
        }
    }

    @Override
    public String getFirstSequenceNumber() {
        assert(!buffers.isEmpty());
        return buffers.get(0).getFirstSequenceNumber();
    }

    @Override
    public String getLastSequenceNumber() {
        assert(!buffers.isEmpty());
        return buffers.get(0).getLastSequenceNumber();
    }

    @Override
    public boolean shouldFlush() {
        assert(!buffers.isEmpty());
        return buffers.get(0).shouldFlush();
    }

    @Override
    public List<T> getRecords() {
        assert(!buffers.isEmpty());
        return buffers.get(0).getRecords();
    }
}
