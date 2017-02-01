package com.amazonaws.services.kinesis.connectors.firehose;

import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchEmitter;
import com.amazonaws.services.kinesisfirehose.model.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class to split a buffer into record batches to batches of Kinesis Firehose records of
 * <= 500 records and <= 4MB in size, as per the Firehose API limits.
 */

class RecordBatcher {

    private static final Log LOG = LogFactory.getLog(ElasticsearchEmitter.class);

    List<List<Record>> makeBatches(List<Record> records) {
        // N.B. the case where a single record is over the byte-size limit is not handled
        // here; it will be rejected by the Firehose API and classed as failed when it is
        // emitted.

        Integer numRecords = 0;
        Integer byteSize = 0;
        List<Record> curBatch = new ArrayList<>();

        List<List<Record>> outRecords = new ArrayList<>();

        for (Record record : records) {

            Integer recSize = record.getData().remaining();

            if (numRecords + 1 > 500 || byteSize + recSize > 4194304) {
                // add current batch to the list and start a new one
                outRecords.add(Collections.unmodifiableList(curBatch));

                curBatch = new ArrayList<>();

                numRecords = 0;
                byteSize = 0;
            }

            curBatch.add(record);

            numRecords++;
            byteSize += recSize;
        }

        if (curBatch.size() > 0) {
            outRecords.add(curBatch);
        }

        LOG.debug(String.format("Built %d batches", outRecords.size()));

        return Collections.unmodifiableList(outRecords);
    }

}
