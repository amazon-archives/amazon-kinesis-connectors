/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.elasticsearch;

import samples.KinesisMessageModel;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchEmitter;
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase;

public class ElasticsearchPipeline implements IKinesisConnectorPipeline<KinesisMessageModel, ElasticsearchObject> {

    @Override
    public IEmitter<ElasticsearchObject> getEmitter(KinesisConnectorConfiguration configuration) {
        return new ElasticsearchEmitter(configuration);
    }

    @Override
    public IBuffer<KinesisMessageModel> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<KinesisMessageModel>(configuration);
    }

    @Override
    public ITransformerBase<KinesisMessageModel, ElasticsearchObject>
            getTransformer(KinesisConnectorConfiguration configuration) {
        if (configuration.BATCH_RECORDS_IN_PUT_REQUEST) {
            return new BatchedKinesisMessageModelElasticsearchTransformer();
        } else {
            return new SingleKinesisMessageModelElasticsearchTransformer();
        }
    }

    @Override
    public IFilter<KinesisMessageModel> getFilter(KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<KinesisMessageModel>();
    }

}
