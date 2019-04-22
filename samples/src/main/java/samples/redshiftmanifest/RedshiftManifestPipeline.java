/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.redshiftmanifest;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.impl.StringToByteArrayTransformer;
import com.amazonaws.services.kinesis.connectors.impl.StringToStringTransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.redshift.RedshiftManifestEmitter;

/**
 * The Pipeline used by the {@link RedshiftManifestExecutor} in the Amazon Redshift manifest sample.
 * Processes Amazon S3 file name records in String format. Uses:
 * <ul>
 * <li>{@link RedshiftManifestEmitter}</li>
 * <li>{@link BasicMemoryBuffer}</li>
 * <li>{@link StringToByteArrayTransformer}</li>
 * <li>{@link AllPassFilter}</li>
 * </ul>
 */
public class RedshiftManifestPipeline implements IKinesisConnectorPipeline<String, String> {

    @Override
    public IEmitter<String> getEmitter(KinesisConnectorConfiguration configuration) {
        return new RedshiftManifestEmitter(configuration);
    }

    @Override
    public IBuffer<String> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<>(configuration);
    }

    @Override
    public ITransformer<String, String> getTransformer(KinesisConnectorConfiguration configuration) {
        return new StringToStringTransformer();
    }

    @Override
    public IFilter<String> getFilter(KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<String>();
    }

}
