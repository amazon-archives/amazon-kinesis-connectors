package com.amazonaws.services.kinesis.connectors.firehose;

import com.amazonaws.services.kinesisfirehose.model.Record;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;


public class RecordBatcherTest {
    
    private RecordBatcher batcher = new RecordBatcher();

    private List<Record> generateRecords(Integer n) {
        List<Record> records = new ArrayList<>();

        for (Integer i = 0; i < n; i++) {
            records.add(new Record().withData(ByteBuffer.wrap(i.toString().getBytes())));
        }

        return Collections.unmodifiableList(records);
    }

    @Test
    public void testShouldReturnEmptyListForNoRecords() {

        List<List<Record>> batches = batcher.makeBatches(new ArrayList<Record>());
        Assert.assertEquals(0, batches.size());

    }

    @Test
    public void testShouldSplitRecordsByCountForMultipleBatches() {

        List<Record> inRecords = generateRecords(1250);

        List<List<Record>> batches = batcher.makeBatches(inRecords);

        Assert.assertEquals(3, batches.size());
        Assert.assertEquals(500, batches.get(0).size());
        Assert.assertEquals(500, batches.get(1).size());
        Assert.assertEquals(250, batches.get(2).size());
    }

    @Test
    public void testShouldNotSplitRecordsByCountForOneBatch() {

        List<Record> inRecords = generateRecords(123);

        List<List<Record>> batches = batcher.makeBatches(inRecords);

        Assert.assertEquals(1, batches.size());
        Assert.assertEquals(123, batches.get(0).size());
    }

    @Test
    public void testShouldSplitRecordsByTotalSize() {

        byte[] record1mb = new byte[1048576];
        new Random().nextBytes(record1mb);

        byte[] record2mb = new byte[2097152];
        new Random().nextBytes(record2mb);

        List<Record> inRecords = new ArrayList<>();
        inRecords.add(new Record().withData(ByteBuffer.wrap(record1mb)));
        inRecords.add(new Record().withData(ByteBuffer.wrap(record1mb)));
        inRecords.add(new Record().withData(ByteBuffer.wrap(record1mb)));
        inRecords.add(new Record().withData(ByteBuffer.wrap(record2mb)));
        inRecords.add(new Record().withData(ByteBuffer.wrap(record1mb)));

        List<List<Record>> batches = batcher.makeBatches(inRecords);

        Assert.assertEquals(2, batches.size());
        Assert.assertEquals(3, batches.get(0).size());
        Assert.assertEquals(2, batches.get(1).size());
    }

}
