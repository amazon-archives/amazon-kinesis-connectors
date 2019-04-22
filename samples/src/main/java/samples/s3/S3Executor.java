/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.s3;

import samples.KinesisConnectorExecutor;
import samples.KinesisMessageModel;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;

/**
 * Executor to emit records to Amazon S3 files. The number of records per Amazon S3 file can be set in the buffer
 * properties.
 */
public class S3Executor extends KinesisConnectorExecutor<KinesisMessageModel, byte[]> {
    private static final String CONFIG_FILE = "S3Sample.properties";

    /**
     * Creates a new S3Executor.
     * 
     * @param configFile
     *        The name of the configuration file to look for on the classpath
     */
    public S3Executor(String configFile) {
        super(configFile);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<KinesisMessageModel, byte[]>
            getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<KinesisMessageModel, byte[]>(new S3Pipeline(), this.config);
    }

    /**
     * Main method to run the S3Executor.
     * 
     * @param args
     */
    public static void main(String[] args) {
        KinesisConnectorExecutor<KinesisMessageModel, byte[]> s3Executor = new S3Executor(CONFIG_FILE);
        s3Executor.run();
    }
}
