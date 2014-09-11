/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package samples.elasticsearch;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import samples.KinesisMessageModel;

import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject;
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchTransformer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Extends ElasticsearchTransformer for {@link KinesisMessageModel}. Provides implementation for fromClass by
 * transforming the record into JSON format and setting the index, type and id to use for Elasticsearch.
 * 
 * Abstract to give same implementation of fromClass for both Batched and Single processing scenarios.
 */
public abstract class KinesisMessageModelElasticsearchTransformer extends ElasticsearchTransformer<KinesisMessageModel> {
    private static final Log LOG = LogFactory.getLog(KinesisMessageModelElasticsearchTransformer.class);

    private static final String INDEX_NAME = "kinesis-example";

    @Override
    public ElasticsearchObject fromClass(KinesisMessageModel record) throws IOException {
        String index = INDEX_NAME;
        String type = record.getClass().getSimpleName();
        String id = Integer.toString(record.getUserid());
        String source = null;
        boolean create = true;
        try {
            source = new ObjectMapper().writeValueAsString(record);
        } catch (JsonProcessingException e) {
            String message = "Error parsing record to JSON";
            LOG.error(message, e);
            throw new IOException(message, e);
        }

        ElasticsearchObject elasticsearchObject = new ElasticsearchObject(index, type, id, source);
        elasticsearchObject.setCreate(create);

        return elasticsearchObject;
    }
}
