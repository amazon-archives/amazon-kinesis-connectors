/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.redshiftmanifest;

import samples.KinesisMessageModel;
import samples.redshiftbasic.KinesisMessageModelRedshiftTransformer;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.s3.S3ManifestEmitter;

/**
 * The Pipeline used by the {@link S3ManifestExecutor} in the Amazon Redshift manifest sample. Processes
 * KinesisMessageModel records in JSON String format and outputs Amazon S3 file name records in String
 * format. Uses:
 * <ul>
 * <li>{@link S3ManifestEmitter}</li>
 * <li>{@link BasicMemoryBuffer}</li>
 * <li>{@link KinesisMessageModelRedshiftTransformer}</li>
 * <li>{@link AllPassFilter}</li>
 * </ul>
 */
public class S3ManifestPipeline implements IKinesisConnectorPipeline<KinesisMessageModel, byte[]> {

    @Override
    public IEmitter<byte[]> getEmitter(KinesisConnectorConfiguration configuration) {
        return new S3ManifestEmitter(configuration);
    }

    @Override
    public IBuffer<KinesisMessageModel> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<KinesisMessageModel>(configuration);
    }

    @Override
    public ITransformer<KinesisMessageModel, byte[]> getTransformer(KinesisConnectorConfiguration configuration) {
        return new KinesisMessageModelRedshiftTransformer(configuration);
    }

    @Override
    public IFilter<KinesisMessageModel> getFilter(KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<KinesisMessageModel>();
    }

}
