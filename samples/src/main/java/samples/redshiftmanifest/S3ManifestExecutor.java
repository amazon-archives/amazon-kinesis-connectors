/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.redshiftmanifest;

import samples.KinesisConnectorExecutor;
import samples.KinesisMessageModel;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;

/**
 * The executor used to emit files to Amazon S3 and then put the file names to a secondary Amazon Kinesis stream.
 */
public class S3ManifestExecutor extends KinesisConnectorExecutor<KinesisMessageModel, byte[]> {

    /**
     * Creates a new S3ManifestExecutor.
     * 
     * @param configFile
     *        The name of the configuration file to look for on the classpath
     */
    public S3ManifestExecutor(String configFile) {
        super(configFile);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<KinesisMessageModel, byte[]>
            getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<>(new S3ManifestPipeline(), config);
    }
}
