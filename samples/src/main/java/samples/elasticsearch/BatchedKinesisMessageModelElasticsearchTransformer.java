/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.elasticsearch;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import samples.KinesisMessageModel;

import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject;
import com.amazonaws.services.kinesis.connectors.interfaces.ICollectionTransformer;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Extends KinesisMessageModelElasticsearchTransformer and implements ICollectionTransformer
 * to provide a toClass method to transform an Amazon Kinesis Record that contains multiple
 * instances of KinesisMessageModel.
 * 
 * To see how these records were batched, view {@class samples.BatchedStreamSource}.
 */
public class BatchedKinesisMessageModelElasticsearchTransformer extends KinesisMessageModelElasticsearchTransformer
        implements ICollectionTransformer<KinesisMessageModel, ElasticsearchObject> {
    private static final Log LOG = LogFactory.getLog(BatchedKinesisMessageModelElasticsearchTransformer.class);

    @SuppressWarnings("unchecked")
    @Override
    public Collection<KinesisMessageModel> toClass(Record record) throws IOException {

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(record.getData().array()))) {
            return (Collection<KinesisMessageModel>) ois.readObject();
        } catch (Exception e) {
            String message = "Error reading object from ObjectInputStream: " + new String(record.getData().array());
            LOG.error(message, e);
            throw new IOException(message, e);
        }
    }

}
