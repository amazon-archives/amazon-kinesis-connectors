/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.connectors;

import java.util.ArrayList;
import java.util.List;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;

public class UnmodifiableBufferTests {
    IMocksControl control;
    IBuffer<Integer> buffer;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        control = EasyMock.createControl();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUnmodifiableBuffer() {
        long bytesToBuffer = 10;
        long recordsToBuffer = 10;
        String firstSequenceNumber = "firstSequenceNumber";
        String lastSequenceNumber = "lastSequenceNumber";
        List<Integer> records = new ArrayList<Integer>();

        buffer = (IBuffer<Integer>) control.createMock(IBuffer.class);
        control.reset();

        // Modifiable Buffer Behavior
        EasyMock.expect(buffer.getBytesToBuffer()).andReturn(bytesToBuffer);
        EasyMock.expect(buffer.getNumRecordsToBuffer()).andReturn(recordsToBuffer);
        EasyMock.expect(buffer.getFirstSequenceNumber()).andReturn(firstSequenceNumber);
        EasyMock.expect(buffer.getLastSequenceNumber()).andReturn(lastSequenceNumber);
        EasyMock.expect(buffer.getRecords()).andReturn(records);
        control.replay();

        UnmodifiableBuffer<Integer> umb = new UnmodifiableBuffer<Integer>(buffer);

        // Test getters behave like buffer
        assertEquals(umb.getBytesToBuffer(), bytesToBuffer);
        assertEquals(umb.getNumRecordsToBuffer(), recordsToBuffer);
        assertEquals(umb.getFirstSequenceNumber(), firstSequenceNumber);
        assertEquals(umb.getLastSequenceNumber(), lastSequenceNumber);
        List<Integer> testRecords = umb.getRecords();
        // Verify buffer methods were called
        control.verify();

        // Expect exceptions on the following operations
        exception.expect(UnsupportedOperationException.class);
        umb.consumeRecord(1, 1, "");

        exception.expect(UnsupportedOperationException.class);
        umb.clear();

        exception.expect(UnsupportedOperationException.class);
        testRecords.clear();
    }
}
