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
package com.amazonaws.services.kinesis.connectors.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;


import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;

public class BasicMemoryBufferTests {
    KinesisConnectorConfiguration config;

    IMocksControl control;
    int buffRecCount = 1000;
    int buffByteLim = 1024 * 1024;
    long buffTimeMilliLim = 1500;

    @Before
    public void setUp() throws Exception {
        // Setup the config
        Properties props = new Properties();
        props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT,
                Integer.toString(buffRecCount));
        props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT,
                Integer.toString(buffByteLim));
        props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT,
                Long.toString(buffTimeMilliLim));
        config = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain());

        control = EasyMock.createControl();

    }

    @Test
    public void testConsumeRecord() {
        int i, iters = buffRecCount + 1; // one more than buffer size
        control.reset();

        // Object under test
        control.replay();
        BasicMemoryBuffer<Integer> buffer = new BasicMemoryBuffer<Integer>(config);
        for (i = 0; i < iters; i++) {
            buffer.consumeRecord(i, 1, Integer.toString(i));
        }
        control.verify();

        // check loop ran correct number of times
        assertEquals(i, iters);

        // Check correct behavior of Buffer
        List<Integer> buf = buffer.getRecords();
        assertTrue(buf != null);
        assertEquals(buf.size(), iters);
        for (i = 0; i < iters; i++) {
            assertEquals(new Integer(i), buf.get(i));
        }
        // check that there are more than 1000 items in the buffer and
        // shouldFlush()
        assertTrue(buffer.shouldFlush());
        // check the sequence numbers
        assertEquals(buffer.getFirstSequenceNumber(), Integer.toString(0));
        assertEquals(buffer.getLastSequenceNumber(), Integer.toString(iters - 1));

        // clear and check that the buffer is empty
        buffer.clear();
        assertTrue(buffer.getRecords().isEmpty());
    }

    @Test
    public void testShouldFlush() {
        int i, iters = 512; 
        int recordSize = buffByteLim / 512;
        // Create Filter
        control.reset();

        // Object under test
        control.replay();
        BasicMemoryBuffer<Integer> buffer = new BasicMemoryBuffer<Integer>(config);
        for (i = 0; i < iters; i++) {
            assertFalse("Failed at count " + i, buffer.shouldFlush());
            buffer.consumeRecord(i, recordSize, Integer.toString(i));
        }
        control.verify();
        assertTrue(buffer.shouldFlush());
        
        // adding test for time buffer
        BasicMemoryBuffer<Integer> buffer2 = new BasicMemoryBuffer<Integer>(config){
            private long currentTime = 0;
            @Override
            protected long getCurrentTimeMilliseconds(){
                long previousTime = currentTime;
                currentTime  += 500; // increment the time by 500
                return previousTime;
            }
        };
        buffer2.clear();
        // put in one records so that neither record count limit nor byte size limit is 
        // reached but buffer is not empty
        buffer2.consumeRecord(1, 1, "1"); 
        assertFalse(buffer2.shouldFlush());
        assertFalse(buffer2.shouldFlush());
        assertTrue(buffer2.shouldFlush());
        buffer2.clear();
        assertFalse(buffer2.shouldFlush());
        assertFalse(buffer2.shouldFlush());
        assertFalse(buffer2.shouldFlush());
        // Since buffer is empty this time, it should not flush even if the time is exceeded
        assertFalse(buffer2.shouldFlush());
    }

}
