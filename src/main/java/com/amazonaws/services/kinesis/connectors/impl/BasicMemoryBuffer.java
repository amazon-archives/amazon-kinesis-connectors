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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;

/**
 * This class is a basic implementation of the IBuffer interface. It is a wrapper on a buffer of
 * records that are periodically flushed. It is configured with an implementation of IFilter that
 * decides whether a record will be added to the buffer to be emitted.
 * 
 * @param <T>
 */
public class BasicMemoryBuffer<T> implements IBuffer<T> {

    private final long bytesPerFlush;
    private final long numMessagesToBuffer;
    private final long millisecondsToBuffer;

    private final List<T> buffer;
    private final AtomicLong byteCount;

    private String firstSequenceNumber;
    private String lastSequenceNumber;

    private long previousFlushTimeMillisecond;

    public BasicMemoryBuffer(KinesisConnectorConfiguration configuration, List<T> buffer) {
        bytesPerFlush = configuration.BUFFER_BYTE_SIZE_LIMIT;
        numMessagesToBuffer = configuration.BUFFER_RECORD_COUNT_LIMIT;
        millisecondsToBuffer = configuration.BUFFER_MILLISECONDS_LIMIT;
        this.buffer = buffer;
        byteCount = new AtomicLong();
        previousFlushTimeMillisecond = getCurrentTimeMilliseconds();
    }

    public BasicMemoryBuffer(KinesisConnectorConfiguration configuration) {
        this(configuration, new LinkedList<T>());
    }

    @Override
    public long getBytesToBuffer() {
        return bytesPerFlush;
    }

    @Override
    public long getNumRecordsToBuffer() {
        return numMessagesToBuffer;
    }

    @Override
    public long getMillisecondsToBuffer() {
        return millisecondsToBuffer;
    }

    @Override
    public void consumeRecord(T record, int recordSize, String sequenceNumber) {
        if (buffer.isEmpty()) {
            firstSequenceNumber = sequenceNumber;
        }
        lastSequenceNumber = sequenceNumber;
        buffer.add(record);
        byteCount.addAndGet(recordSize);
    }

    @Override
    public void clear() {
        buffer.clear();
        byteCount.set(0);
        previousFlushTimeMillisecond = getCurrentTimeMilliseconds();
    }

    @Override
    public String getFirstSequenceNumber() {
        return firstSequenceNumber;
    }

    @Override
    public String getLastSequenceNumber() {
        return lastSequenceNumber;
    }

    /**
     * By default, we flush once we have exceeded the number of messages or maximum bytes to buffer.
     * However, subclasses can use their own means to determine if they should flush.
     * 
     * @return true if either the number of records in the buffer exceeds max number of records or
     *         the size of the buffer exceeds the max number of bytes in the buffer.
     */
    @Override
    public boolean shouldFlush() {
        long timelapseMillisecond = getCurrentTimeMilliseconds() - previousFlushTimeMillisecond;
        return (!buffer.isEmpty())
                && ((buffer.size() >= getNumRecordsToBuffer()) || (byteCount.get() >= getBytesToBuffer()) || (timelapseMillisecond >= getMillisecondsToBuffer()));
    }

    @Override
    public List<T> getRecords() {
        return buffer;
    }

    // This method has protected access for unit testing purposes.
    protected long getCurrentTimeMilliseconds() {
        return System.currentTimeMillis();
    }

}
