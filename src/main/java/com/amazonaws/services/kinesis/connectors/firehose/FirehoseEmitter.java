package com.amazonaws.services.kinesis.connectors.firehose;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Emitter to connect a Kinesis Stream to a Kinesis Firehose stream
 */
public class FirehoseEmitter implements IEmitter<Record> {
    private static final Log LOG = LogFactory.getLog(FirehoseEmitter.class);

    private final AmazonKinesisFirehoseAsync firehoseClient;
    private final String streamName;
    private final String streamRegion;

    public FirehoseEmitter(KinesisConnectorConfiguration configuration) {
        firehoseClient = AmazonKinesisFirehoseAsyncClientBuilder
                .defaultClient();

        streamName = configuration.FIREHOSE_STREAM_NAME;
        streamRegion = configuration.FIREHOSE_STREAM_REGION;
    }

    /**
     * Emit a record buffer
     * @param buffer
     *        The full buffer of records
     * @return List<Record> A list of any records that were not accepted by Kinesis Firehose
     * @throws IOException
     */
    public List<Record> emit(UnmodifiableBuffer<Record> buffer) throws IOException {

        List<Record> records = buffer.getRecords();

        List<List<Record>> batches = new RecordBatcher().makeBatches(records);

        ArrayList<Future<PutRecordBatchResult>> resultFutures = new ArrayList<>();

        Integer i = 0;
        for (List<Record> batch : batches) {
            LOG.info(String.format("Writing %d records to firehose stream (batch %d of %d)", batch.size(), ++i, batches.size()));

            resultFutures.add(firehoseClient.putRecordBatchAsync(new PutRecordBatchRequest()
                    .withRecords(batch)
                    .withDeliveryStreamName(streamName)));
        }

        List<Record> failures = new ArrayList<>();

        for (i = 0; i < resultFutures.size(); i++) {
            try {
                PutRecordBatchResult res = resultFutures.get(i).get();

                if (res.getFailedPutCount() > 0) {
                    List<PutRecordBatchResponseEntry> entries = res.getRequestResponses();

                    for (Integer j = 0; j < entries.size(); j++) {
                        PutRecordBatchResponseEntry entry = entries.get(j);
                        if (entry.getErrorCode() != null) {
                            failures.add(batches.get(i).get(j));
                        }
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn(String.format("Caught %s while putting batch %d, %d records will be failed",
                        e.getClass().getName(),
                        i, batches.get(i).size()));
                LOG.warn("Exception details: ", e);

                // This whole batch of records to the failure list
                failures.addAll(batches.get(i));
            }
        }

        if (!failures.isEmpty()) {
            LOG.warn(String.format("%d records failed", failures.size()));
        }
        return failures;
    }

    public void fail(List<Record> records) {
        for (Record record : records) {
            LOG.error("Record failed: " + record);
        }
    }

    public void shutdown() {}
}
