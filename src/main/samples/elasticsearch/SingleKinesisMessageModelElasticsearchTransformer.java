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
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Extends KinesisMessageModelElasticsearchTransformer and implements ITransformer
 * to provide a toClass method to transform an Amazon Kinesis Record into a KinesisMessageModel.
 * 
 * To see how this record was put, view {@class samples.StreamSource}.
 */
public class SingleKinesisMessageModelElasticsearchTransformer extends KinesisMessageModelElasticsearchTransformer
        implements ITransformer<KinesisMessageModel, ElasticsearchObject> {
    private static final Log LOG = LogFactory.getLog(SingleKinesisMessageModelElasticsearchTransformer.class);

    @Override
    public KinesisMessageModel toClass(Record record) throws IOException {
        try {
            return new ObjectMapper().readValue(record.getData().array(), KinesisMessageModel.class);
        } catch (IOException e) {
            String message = "Error parsing record from JSON: " + new String(record.getData().array());
            LOG.error(message, e);
            throw new IOException(message, e);
        }
    }

}
