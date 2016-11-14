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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessor;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.ICollectionTransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase;
import com.amazonaws.services.kinesis.model.Record;

public class KinesisConnectorRecordProcessorTests {
    // control object used to create mock dependencies
    IMocksControl control;

    // Dependencies to be mocked
    IEmitter<Object> emitter;
    ITransformer<Object, Object> transformer;
    IBuffer<Object> buffer;
    IFilter<Object> filter;
    IRecordProcessorCheckpointer checkpointer;
    KinesisConnectorConfiguration configuration;

    // used when generating dummy records and verifying behavior
    int DEFAULT_RECORD_BYTE_SIZE = 4;
    String DEFAULT_PARTITION_KEY = "";
    String DEFAULT_SEQUENCE_NUMBER = "";

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        // control object used to create mock dependencies
        control = EasyMock.createControl();

        // mock dependencies
        emitter = control.createMock(IEmitter.class);
        transformer = control.createMock(ITransformer.class);
        buffer = control.createMock(IBuffer.class);
        filter = control.createMock(IFilter.class);
        checkpointer = control.createMock(IRecordProcessorCheckpointer.class);
        // use a real configuration to get actual default values (not anything created by EasyMock)
        configuration = new KinesisConnectorConfiguration(new Properties(), new DefaultAWSCredentialsProviderChain());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullBuffer() {
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                null, filter, emitter, transformer, configuration);
    }
    @Test(expected = IllegalArgumentException.class)
    public void testNullFilter() {
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, null, emitter, transformer, configuration);
    }
    @Test(expected = IllegalArgumentException.class)
    public void testNullEmitter() {
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, filter, null, transformer, configuration);
    }
    @Test(expected = IllegalArgumentException.class)
    public void testNullTransformer() {
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, filter, emitter, null, configuration);
    }

    /**
     * Test exception thrown when calling processRecords before initialize(
     */
    @Test(expected = IllegalStateException.class)
    public void testProcessRecordsCalledBeforeInitialize() {
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, filter, emitter, transformer, configuration);
        kcrp.processRecords(Collections.EMPTY_LIST, checkpointer);
    }

     /**
      * Test process records under normal conditions.
      */
    @Test
    @SuppressWarnings("unchecked")
    public void testProcessRecords() throws ThrottlingException, ShutdownException, IOException,
            KinesisClientLibDependencyException, InvalidStateException {
        // Test Variables
        Object dummyRecord = new Object();
        int numRecords = 5;
        String shardId = "shardId";

        // reset the control to mock new behavior
        control.reset();

        // Transformer Behavior:
        // transform numRecords records into dummyRecord objects
        EasyMock.expect(transformer.toClass(EasyMock.anyObject(Record.class))).andReturn(dummyRecord);
        EasyMock.expectLastCall().times(numRecords -1).andReturn(dummyRecord);

        // Filter Behavior:
        // return true for all records
        EasyMock.expect(filter.keepRecord(dummyRecord)).andReturn(true);
        EasyMock.expectLastCall().times(numRecords -1).andReturn(true);

        // Mock arguments
        Object o = EasyMock.anyObject();
        int buf = EasyMock.anyInt();

        // Buffer Behavior:
        // consume numRecords dummy records
        buffer.consumeRecord(o, buf, EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().times(numRecords);
        buffer.getLastSequenceNumber();
        EasyMock.expectLastCall().andReturn(DEFAULT_SEQUENCE_NUMBER);
        buffer.clear();
        EasyMock.expectLastCall();

        // check full buffer and return true
        EasyMock.expect(buffer.shouldFlush()).andReturn(true);

        // call buffer.getRecords
        List<Object> objects = new ArrayList<>();
        Object item = new Object();
        objects.add(item);
        EasyMock.expect(buffer.getRecords()).andReturn(objects);
        EasyMock.expect(transformer.fromClass(item)).andReturn(item);

        // Emitter behavior:
        // one call to emit
        EasyMock.expect(emitter.emit(EasyMock.anyObject(UnmodifiableBuffer.class))).andReturn(
                Collections.emptyList());

        // Checkpointer Behavior:
        // one call to checkpoint
        checkpointer.checkpoint(DEFAULT_SEQUENCE_NUMBER);
        EasyMock.expectLastCall();

        // Initialize class under test
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, filter, emitter, transformer, configuration);
        kcrp.initialize(shardId);

        // Prepare controller for method call
        control.replay();

        // call method
        kcrp.processRecords(getDummyRecordList(numRecords), checkpointer);

        control.verify();
    }

    /**
     * Test emitter throws exception upon processing.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFailRecords() throws IOException, KinesisClientLibDependencyException,
            InvalidStateException, ThrottlingException, ShutdownException {
        Object dummyRecord = new Object();
        int numRecords = 5;
        String shardId = "shardId";

        // reset the control to mock new behavior
        control.reset();

        // Transformer Behavior:
        // transform numRecords records into dummyRecord objects
        EasyMock.expect(transformer.toClass(EasyMock.anyObject(Record.class))).andReturn(dummyRecord);
        EasyMock.expectLastCall().times(numRecords - 1).andReturn(dummyRecord);

        // Filter Behavior:
        // return true for all records
        EasyMock.expect(filter.keepRecord(dummyRecord)).andReturn(true);
        EasyMock.expectLastCall().times(numRecords -1).andReturn(true);

        // Mock arguments
        Object o = EasyMock.anyObject();
        int buf = EasyMock.anyInt();
        String seq = EasyMock.anyObject(String.class);

        // Buffer Behavior:
        // consume numRecords dummy records
        buffer.consumeRecord(o, buf, seq);
        EasyMock.expectLastCall().times(numRecords);

        // check full buffer and return true
        EasyMock.expect(buffer.shouldFlush()).andReturn(true);

        // call buffer.getRecords
        EasyMock.expect(buffer.getRecords()).andReturn(Collections.emptyList());

        // Emitter behavior:
        // one call to emit
        EasyMock.expect(emitter.emit(EasyMock.anyObject(UnmodifiableBuffer.class))).andThrow(
                new IOException());
        emitter.fail(EasyMock.anyObject(List.class));
        EasyMock.expectLastCall();

        // Initialize class under test
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, filter, emitter, transformer, configuration);
        kcrp.initialize(shardId);

        // Prepare controller for method call
        control.replay();

        // call method
        kcrp.processRecords(getDummyRecordList(numRecords), checkpointer);

        control.verify();
    }

    /**
      * Test process records under normal conditions but with batch processor.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testProcessBatchedRecords() throws ThrottlingException, ShutdownException, IOException,
    KinesisClientLibDependencyException, InvalidStateException {
        // Test Variables
        Object dummyRecord1 = new Object();
        Object dummyRecord2= new Object();
        List<Object> dummyCollection = new ArrayList<>();
        dummyCollection.add(dummyRecord1);
        dummyCollection.add(dummyRecord2);

        // 5 Kinesis records, with 2 objects per. So 10 total records
        int numRecords = 5;
        int numTotalRecords = numRecords * 2;
        String shardId = "shardId";
        // Override existing transformer with a collection transformer
        ICollectionTransformer<Object,Object> collectionTransformer = control.createMock(ICollectionTransformer.class);

        // reset the control to mock new behavior
        control.reset();

        // Transformer Behavior:
        // transform numRecords records into dummyRecord objects
        EasyMock.expect(collectionTransformer.toClass(EasyMock.anyObject(Record.class))).andReturn(dummyCollection);
        EasyMock.expectLastCall().times(numRecords -1).andReturn(dummyCollection);

        // Filter Behavior:
        // return true for all records
        EasyMock.expect(filter.keepRecord(dummyRecord1)).andReturn(true);
        EasyMock.expectLastCall().times(numRecords -1).andReturn(true);
        EasyMock.expect(filter.keepRecord(dummyRecord2)).andReturn(true);
        EasyMock.expectLastCall().times(numRecords -1).andReturn(true);

        // Mock arguments
        Object o = EasyMock.anyObject();
        int buf = EasyMock.anyInt();

        // Buffer Behavior:
        // consume numRecords dummy records
        buffer.consumeRecord(o, buf, EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().times(numTotalRecords);
        buffer.getLastSequenceNumber();
        EasyMock.expectLastCall().andReturn(DEFAULT_SEQUENCE_NUMBER);
        buffer.clear();
        EasyMock.expectLastCall();

        // check full buffer and return true
        EasyMock.expect(buffer.shouldFlush()).andReturn(true);

        // call buffer.getRecords
        EasyMock.expect(buffer.getRecords()).andReturn(Collections.emptyList());

        // Emitter behavior:
        // one call to emit
        EasyMock.expect(emitter.emit(EasyMock.anyObject(UnmodifiableBuffer.class))).andReturn(
                Collections.emptyList());

        // Checkpointer Behavior:
        // one call to checkpoint
        checkpointer.checkpoint(DEFAULT_SEQUENCE_NUMBER);
        EasyMock.expectLastCall();

        // Initialize class under test
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, filter, emitter, collectionTransformer, configuration);
        kcrp.initialize(shardId);

        // Prepare controller for method call
        control.replay();

        // call method
        kcrp.processRecords(getDummyRecordList(numRecords), checkpointer);

        control.verify();
    }

    /**
     * Test retry logic only retries unprocessed/failed records
     */
    @Test
    public void testRetryBehavior() throws IOException, KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        // Test Variables
        Object dummyRecord1 = new Object();
        Object dummyRecord2= new Object();
        List<Object> objectsAsList = new ArrayList<Object>();
        objectsAsList.add(dummyRecord1);
        objectsAsList.add(dummyRecord2);

        List<Object> singleObjectAsList = new ArrayList<Object>();
        singleObjectAsList.add(dummyRecord1);
        String shardId = "shardId";

        Properties props = new Properties();
        // set backoff interval to 0 to speed up test
        props.setProperty(KinesisConnectorConfiguration.PROP_BACKOFF_INTERVAL, String.valueOf(0));
        // set retry limit to allow for retry below
        props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, String.valueOf(2));
        configuration = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain());

        // reset the control to mock new behavior
        control.reset();

        // set expectations for each record
        EasyMock.expect(transformer.toClass(EasyMock.anyObject(Record.class))).andReturn(dummyRecord1);
        EasyMock.expect(filter.keepRecord(dummyRecord1)).andReturn(true);
        buffer.consumeRecord(dummyRecord1, DEFAULT_RECORD_BYTE_SIZE, DEFAULT_SEQUENCE_NUMBER);

        EasyMock.expect(transformer.toClass(EasyMock.anyObject(Record.class))).andReturn(dummyRecord2);
        EasyMock.expect(filter.keepRecord(dummyRecord2)).andReturn(true);
        buffer.consumeRecord(dummyRecord2, DEFAULT_RECORD_BYTE_SIZE, DEFAULT_SEQUENCE_NUMBER);

        EasyMock.expect(buffer.shouldFlush()).andReturn(true);

        // call Buffer.getRecords
        EasyMock.expect(buffer.getRecords()).andReturn(objectsAsList);

        // Transform back
        EasyMock.expect(transformer.fromClass(dummyRecord1)).andReturn(dummyRecord1);
        EasyMock.expect(transformer.fromClass(dummyRecord2)).andReturn(dummyRecord2);

        // Emitter behavior:
        // one call to emit which fails (test a transient issue), and then returns empty on second call

        // uses the original list (i.e. emitItems)
        UnmodifiableBuffer<Object> unmodBuffer = new UnmodifiableBuffer<>(buffer, objectsAsList);
        EasyMock.expect(emitter.emit(EasyMock.eq(unmodBuffer))).andReturn(singleObjectAsList);
        // uses the returned list (i.e. unprocessed)
        unmodBuffer = new UnmodifiableBuffer<>(buffer, singleObjectAsList);
        EasyMock.expect(emitter.emit(EasyMock.eq(unmodBuffer))).andReturn(Collections.emptyList());

        // Done, so expect buffer clear and checkpoint
        buffer.getLastSequenceNumber();
        EasyMock.expectLastCall().andReturn(DEFAULT_SEQUENCE_NUMBER);
        buffer.clear();
        EasyMock.expectLastCall();
        checkpointer.checkpoint(DEFAULT_SEQUENCE_NUMBER);
        EasyMock.expectLastCall();

        // Initialize class under test
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, filter, emitter, transformer, configuration);
        kcrp.initialize(shardId);

        // Prepare controller for method call
        control.replay();

        // call method
        kcrp.processRecords(getDummyRecordList(objectsAsList.size()), checkpointer);

        control.verify();
    }
    /**
     * Test fail called when all retries done.
     */
    @Test
    public void testFailAfterRetryLimitReached() throws IOException, KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        // Test Variables
        Object dummyRecord1 = new Object();
        Object dummyRecord2= new Object();
        List<Object> objectsAsList = new ArrayList<Object>();
        objectsAsList.add(dummyRecord1);
        objectsAsList.add(dummyRecord2);

        List<Object> singleObjectAsList = new ArrayList<Object>();
        singleObjectAsList.add(dummyRecord1);
        String shardId = "shardId";

        Properties props = new Properties();
        // set backoff interval to 0 to speed up test
        props.setProperty(KinesisConnectorConfiguration.PROP_BACKOFF_INTERVAL, String.valueOf(0));
        // set retry limit to allow for retry below (1)
        props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, String.valueOf(1));
        configuration = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain());

        // reset the control to mock new behavior
        control.reset();

        // set expectations for each record
        EasyMock.expect(transformer.toClass(EasyMock.anyObject(Record.class))).andReturn(dummyRecord1);
        EasyMock.expect(filter.keepRecord(dummyRecord1)).andReturn(true);
        buffer.consumeRecord(dummyRecord1, DEFAULT_RECORD_BYTE_SIZE, DEFAULT_SEQUENCE_NUMBER);

        EasyMock.expect(transformer.toClass(EasyMock.anyObject(Record.class))).andReturn(dummyRecord2);
        EasyMock.expect(filter.keepRecord(dummyRecord2)).andReturn(true);
        buffer.consumeRecord(dummyRecord2, DEFAULT_RECORD_BYTE_SIZE, DEFAULT_SEQUENCE_NUMBER);

        EasyMock.expect(buffer.shouldFlush()).andReturn(true);

        // call Buffer.getRecords
        EasyMock.expect(buffer.getRecords()).andReturn(objectsAsList);

        // Transform back
        EasyMock.expect(transformer.fromClass(dummyRecord1)).andReturn(dummyRecord1);
        EasyMock.expect(transformer.fromClass(dummyRecord2)).andReturn(dummyRecord2);

        // Emitter behavior:
        // one call to emit which fails (test a transient issue), and then returns empty on second call

        // uses the original list (i.e. emitItems)
        UnmodifiableBuffer<Object> unmodBuffer = new UnmodifiableBuffer<>(buffer, objectsAsList);
        EasyMock.expect(emitter.emit(EasyMock.eq(unmodBuffer))).andReturn(singleObjectAsList);

        // only one retry, so now we should expect fail to be called.
        emitter.fail(singleObjectAsList);

        // Done, so expect buffer clear and checkpoint
        buffer.getLastSequenceNumber();
        EasyMock.expectLastCall().andReturn(DEFAULT_SEQUENCE_NUMBER);
        buffer.clear();
        EasyMock.expectLastCall();
        checkpointer.checkpoint(DEFAULT_SEQUENCE_NUMBER);
        EasyMock.expectLastCall();

        // Initialize class under test
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, filter, emitter, transformer, configuration);
        kcrp.initialize(shardId);

        // Prepare controller for method call
        control.replay();

        // call method
        kcrp.processRecords(getDummyRecordList(objectsAsList.size()), checkpointer);

        control.verify();
    }
    /**
     * Test process records with ITransformerBase should throw exception.
    */
    @Test(expected = RuntimeException.class)
    @SuppressWarnings("unchecked")
    public void testBadTransformer() throws ThrottlingException, ShutdownException, IOException,
    KinesisClientLibDependencyException, InvalidStateException {
        // Test Variables
        String shardId = "shardId";
        int numRecords = 5;

        // Override existing transformer with a collection transformer
        ITransformerBase<Object,Object> baseTransformer = control.createMock(ITransformerBase.class);

        // Initialize class under test
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, filter, emitter, baseTransformer, configuration);
        kcrp.initialize(shardId);

        // call method, expect exception thrown
        kcrp.processRecords(getDummyRecordList(numRecords), checkpointer);
    }

    /**
     * expect nothing to happen on ShutdownReason.ZOMBIE
     */
    @Test
    public void testShutdownZombie() {
        // reset the control to mock new behavior
        control.reset();
        // expect shutdown to be called on emitter
        emitter.shutdown();
        EasyMock.expectLastCall();

        // Prepare controller for method call
        control.replay();

        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, filter, emitter, transformer, configuration);
        kcrp.shutdown(checkpointer, ShutdownReason.ZOMBIE);

        control.verify();
    }

    /**
     * expect buffer flush, emit and checkpoint to happen on ShutdownReason.TERMINATE
     */
    @Test
    public void testShutdownTerminate() throws IOException, KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        // reset the control to mock new behavior
        control.reset();

        // expect flush cycle.
        // Get records from buffer, emit, clear, then checkpoint
        EasyMock.expect(buffer.getRecords()).andReturn(Collections.emptyList());
        EasyMock.expect(emitter.emit(EasyMock.anyObject(UnmodifiableBuffer.class))).andReturn(
                Collections.emptyList());
        buffer.getLastSequenceNumber();
        EasyMock.expectLastCall().andReturn(null);
        buffer.clear();
        EasyMock.expectLastCall();
        checkpointer.checkpoint();
        EasyMock.expectLastCall();

        // expect shutdown to be called on emitter
        emitter.shutdown();
        EasyMock.expectLastCall();

        // Prepare controller for method call
        control.replay();

        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessor<Object, Object>(
                buffer, filter, emitter, transformer, configuration);
        kcrp.shutdown(checkpointer, ShutdownReason.TERMINATE);

        control.verify();
    }

    private List<Record> getDummyRecordList(int length) {
        ArrayList<Record> list = new ArrayList<Record>();
        for (int i = 0; i < length; i++) {
            Record dummyRecord = new Record();
            ByteBuffer byteBuffer = ByteBuffer.allocate(DEFAULT_RECORD_BYTE_SIZE);
            dummyRecord.setData(byteBuffer);
            dummyRecord.setPartitionKey(DEFAULT_PARTITION_KEY);
            dummyRecord.setSequenceNumber(DEFAULT_SEQUENCE_NUMBER);
            list.add(dummyRecord);
        }

        return list;
    }

}
