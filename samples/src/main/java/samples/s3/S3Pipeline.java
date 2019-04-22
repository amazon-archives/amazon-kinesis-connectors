/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.s3;

import samples.KinesisMessageModel;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.impl.JsonToByteArrayTransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.s3.S3Emitter;

/**
 * The Pipeline used by the Amazon S3 sample. Processes KinesisMessageModel records in JSON String
 * format. Uses:
 * <ul>
 * <li>S3Emitter</li>
 * <li>BasicMemoryBuffer</li>
 * <li>BasicJsonTransformer</li>
 * <li>AllPassFilter</li>
 * </ul>
 */
public class S3Pipeline implements IKinesisConnectorPipeline<KinesisMessageModel, byte[]> {

    @Override
    public IEmitter<byte[]> getEmitter(KinesisConnectorConfiguration configuration) {
        return new S3Emitter(configuration);
    }

    @Override
    public IBuffer<KinesisMessageModel> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<KinesisMessageModel>(configuration);
    }

    @Override
    public ITransformer<KinesisMessageModel, byte[]> getTransformer(KinesisConnectorConfiguration configuration) {
        return new JsonToByteArrayTransformer<KinesisMessageModel>(KinesisMessageModel.class);
    }

    @Override
    public IFilter<KinesisMessageModel> getFilter(KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<KinesisMessageModel>();
    }

}
