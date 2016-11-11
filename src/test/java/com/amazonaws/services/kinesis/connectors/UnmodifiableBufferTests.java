/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
