/*
 * Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.connectors;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;

/**
 * This class is a wrapper on an IBuffer that limits the functionality of the buffer. This buffer
 * cannot be added to, and retrieving the list of records returns an unmodifiable list. Calling
 * consumeRecord() or clear() will cause an UnathorizedOperationException to be thrown. Calling
 * getRecords() returns the records wrapped in an UnmodifiableList.
 * 
 * @param <T>
 */
public class UnmodifiableBuffer<T> implements IBuffer<T> {

    private final IBuffer<?> buf;
    private final List<T> records;

    public UnmodifiableBuffer(IBuffer<T> buf) {
        this.buf = buf;
        this.records = buf.getRecords();
    }

    public UnmodifiableBuffer(IBuffer<?> buf, List<T> records) {
        this.buf = buf;
        this.records = records;
    }

    @Override
    public long getBytesToBuffer() {
        return buf.getBytesToBuffer();
    }

    @Override
    public long getNumRecordsToBuffer() {
        return buf.getNumRecordsToBuffer();
    }

    @Override
    public long getMillisecondsToBuffer() {
        return buf.getMillisecondsToBuffer();
    }

    @Override
    public boolean shouldFlush() {
        return buf.shouldFlush();
    }

    @Override
    public void consumeRecord(T record, int recordBytes, String sequenceNumber) {
        throw new UnsupportedOperationException("This is an unmodifiable buffer");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This is an unmodifiable buffer");
    }

    @Override
    public String getFirstSequenceNumber() {
        return buf.getFirstSequenceNumber();
    }

    @Override
    public String getLastSequenceNumber() {
        return buf.getLastSequenceNumber();
    }

    @Override
    public List<T> getRecords() {
        return Collections.unmodifiableList(records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(buf, records);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof UnmodifiableBuffer) {
            UnmodifiableBuffer<?> other = (UnmodifiableBuffer<?>) obj;
            return Objects.equals(buf, other.buf) && Objects.equals(records, records);
        }
        return false;
    }
}
