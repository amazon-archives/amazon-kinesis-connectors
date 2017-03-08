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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;

public class UnmodifiableBufferTest {
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

    @Test
    public void testBuffersCompareRecordsForEquality() {
        ArrayList<Integer> buffer1Records = new ArrayList<>();
        buffer1Records.add(1);
        buffer1Records.add(2);

        ArrayList<Integer> buffer2Records = new ArrayList<>();

        UnmodifiableBuffer<Integer> buffer1 = new UnmodifiableBuffer<Integer>(null, buffer1Records);
        UnmodifiableBuffer<Integer> buffer2 = new UnmodifiableBuffer<Integer>(null, buffer2Records);

        boolean equality = buffer1.equals(buffer2);

        assertThat(equality, is(false));

        buffer2Records.addAll(buffer1Records);

        equality = buffer1.equals(buffer2);

        assertThat(equality, is(true));
    }

    @Test
    public void testBuffersCompareInnerBuffersForEquality() {
        testBuffersUsingInnerBufferEquality(false);
        testBuffersUsingInnerBufferEquality(true);
    }

    private void testBuffersUsingInnerBufferEquality(boolean innerBuffersAreEqual) {
        IBuffer<Object> innerBuffer1 = new EqualityTestBuffer<>(innerBuffersAreEqual);
        IBuffer<Object> innerBuffer2 = new EqualityTestBuffer<>(innerBuffersAreEqual);

        UnmodifiableBuffer<Object> buffer1 = new UnmodifiableBuffer<Object>(innerBuffer1, null);
        UnmodifiableBuffer<Object> buffer2 = new UnmodifiableBuffer<Object>(innerBuffer2, null);

        boolean equality = buffer1.equals(buffer2);

        assertThat(equality, is(innerBuffersAreEqual));
    }

    private class EqualityTestBuffer<T> implements IBuffer<T> {
        private boolean equalsReturns;

        public EqualityTestBuffer(boolean equalsReturns) {
            this.equalsReturns = equalsReturns;
        }

        @Override
        public boolean equals(Object o) {
            return this.equalsReturns;
        }

        @Override
        public long getBytesToBuffer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getNumRecordsToBuffer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getMillisecondsToBuffer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean shouldFlush() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void consumeRecord(T record, int recordBytes, String sequenceNumber) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getFirstSequenceNumber() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getLastSequenceNumber() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<T> getRecords() {
            throw new UnsupportedOperationException();
        }
    }
}
