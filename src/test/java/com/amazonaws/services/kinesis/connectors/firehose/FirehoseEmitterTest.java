package com.amazonaws.services.kinesis.connectors.firehose;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.model.*;
import org.easymock.EasyMockSupport;
import org.easymock.IExpectationSetters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static org.easymock.EasyMock.*;


public class FirehoseEmitterTest {

    private FirehoseEmitter emitter;

    private AmazonKinesisFirehoseAsync firehoseClientMock;

    private UnmodifiableBuffer<Record> buffer = createMock(UnmodifiableBuffer.class);

    @Before
    public void setUp() {
        Properties props = new Properties();
        AWSCredentialsProvider creds = createMock(AWSCredentialsProvider.class);

        KinesisConnectorConfiguration configuration = new KinesisConnectorConfiguration(props, creds);
        emitter = new FirehoseEmitter(configuration);

        firehoseClientMock = createMock(AmazonKinesisFirehoseAsync.class);

        setField(emitter, "firehoseClient", firehoseClientMock);
        setField(emitter, "streamName", "TestStream");
    }

    private void setField(Object target, String fieldName, Object value) {
        try {
            Class<?> clazz = target.getClass();
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private List<Record> generateRecords(Integer n) {
        List<Record> records = new ArrayList<>();

        for (Integer i = 0; i < n; i++) {
            records.add(new Record()
                    .withData(ByteBuffer.wrap(i.toString().getBytes())));
        }

        return Collections.unmodifiableList(records);
    }

    private IExpectationSetters<Future<PutRecordBatchResult>> expectBatchRequestWith(PutRecordBatchRequest req) {
        return expect(firehoseClientMock.putRecordBatchAsync(req));
    }

    private IExpectationSetters<Future<PutRecordBatchResult>> expectBatchRequestWith(List<Record> records) {
        return expectBatchRequestWith(new PutRecordBatchRequest()
                .withRecords(records)
                .withDeliveryStreamName("TestStream"));
    }

    private Future<PutRecordBatchResult> mockFutureOf(PutRecordBatchResult resp) throws InterruptedException, ExecutionException {
        Future<PutRecordBatchResult> f = createMock(Future.class);
        expect(f.get()).andReturn(resp);

        replay(f);
        return f;
    }

    @Test
    public void testNoRecordsCase() throws IOException {
        expect(buffer.getRecords()).andReturn(new ArrayList<Record>());

        replay(firehoseClientMock, buffer);

        List<Record> failures = emitter.emit(buffer);
        Assert.assertTrue(failures.isEmpty());

        verify(firehoseClientMock, buffer);
    }

    @Test
    public void testNullRecordsCase() throws IOException {
        ArrayList<Record> recs = new ArrayList<Record>();
        recs.add(null);
        expect(buffer.getRecords()).andReturn(recs);

        replay(firehoseClientMock, buffer);

        List<Record> failures = emitter.emit(buffer);
        Assert.assertTrue(failures.isEmpty());

        verify(firehoseClientMock, buffer);
    }

    @Test
    public void testWorkingCaseNoFailures() throws IOException, InterruptedException, ExecutionException {
        List<Record> records = generateRecords(1750);
        List<List<Record>> batches = new RecordBatcher().makeBatches(records);

        Future mockFuture = createMock(Future.class);

        expect(buffer.getRecords()).andReturn(records);

        for (List<Record> batch : batches) {

            expectBatchRequestWith(batch)
                    .andReturn(mockFuture);

            expect(mockFuture.get()).andReturn(new PutRecordBatchResult().withFailedPutCount(0));

        }

        replay(firehoseClientMock, buffer, mockFuture);

        List<Record> failures = emitter.emit(buffer);

        verify(firehoseClientMock, buffer, mockFuture);

        Assert.assertTrue(failures.isEmpty());
    }

    @Test
    public void testRecordLevelFailureCase() throws IOException, ExecutionException, InterruptedException {

        List<Record> records = generateRecords(1750);
        expect(buffer.getRecords()).andReturn(records);

        List<List<Record>> batches = new RecordBatcher().makeBatches(records);

        // pick a random record to fail
        Integer failedRecordNum = new Random().nextInt(1750);
        Integer failedRecordBatch = (int)(failedRecordNum.doubleValue()/500.0);

        for (Integer i = 0; i < batches.size(); i++) {

            List<Record> batch = batches.get(i);

            // Prepare PutRecordResponseEntry set
            List<PutRecordBatchResponseEntry> entries = new ArrayList<>();
            Integer failIndex = failedRecordNum - failedRecordBatch * 500; // the record idx in this batch

            for (Integer j = 0; j < batch.size(); j++) {
                if (i.equals(failedRecordBatch) && j.equals(failIndex)) {
                    entries.add(new PutRecordBatchResponseEntry()
                        .withErrorCode("error")
                        .withErrorMessage("uh oh spagetti-o"));
                } else {
                    entries.add(new PutRecordBatchResponseEntry());
                }
            }

            Future resultFuture = createMock(Future.class);

            PutRecordBatchResult res = new PutRecordBatchResult()
                    .withFailedPutCount(0)
                    .withRequestResponses(entries);

            expect(resultFuture.get()).andReturn(res.withFailedPutCount(i.equals(failedRecordBatch) ? 1 : 0));

            expectBatchRequestWith(batches.get(i))
                    .andReturn(resultFuture);

            replay(resultFuture);
        }

        replay(buffer, firehoseClientMock);

        List<Record> failures = emitter.emit(buffer);

        verify(buffer, firehoseClientMock);

        Assert.assertEquals(1, failures.size());
        Assert.assertEquals(failedRecordNum.toString(), new String(failures.get(0).getData().array()));
    }

    @Test
    public void testAsyncFailureCase() throws IOException, ExecutionException, InterruptedException {

        List<Record> records = generateRecords(1300);
        List<List<Record>> batches = new RecordBatcher().makeBatches(records);

        // Three batches, the second one will fail

        expectBatchRequestWith(batches.get(0))
                .andReturn(mockFutureOf(new PutRecordBatchResult().withFailedPutCount(0)));

        Future<PutRecordBatchResult> errorFuture = createMock(Future.class);
        expect(errorFuture.get()).andThrow(new InterruptedException());

        expectBatchRequestWith(batches.get(1))
                .andReturn(errorFuture);

        expectBatchRequestWith(batches.get(2))
                .andReturn(mockFutureOf(new PutRecordBatchResult().withFailedPutCount(0)));

        expect(buffer.getRecords())
                .andReturn(records);

        replay(buffer, errorFuture, firehoseClientMock);

        List<Record> failures = emitter.emit(buffer);

        Assert.assertEquals(500, failures.size());
    }
}
