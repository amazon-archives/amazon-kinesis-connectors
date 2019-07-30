/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.redshiftbasic;

import samples.KinesisConnectorExecutor;
import samples.KinesisMessageModel;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;

/**
 * The Executor for the basic Amazon Redshift emitter sample.
 */
public class RedshiftBasicExecutor extends KinesisConnectorExecutor<KinesisMessageModel, byte[]> {
    private static final String CONFIG_FILE = "RedshiftBasicSample.properties";

    /**
     * Creates a new RedshiftBasicExecutor.
     * 
     * @param configFile The name of the configuration file to look for on the classpath
     */
    public RedshiftBasicExecutor(String configFile) {
        super(configFile);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<KinesisMessageModel, byte[]>
            getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<>(new RedshiftBasicPipeline(), config);
    }

    /**
     * Main method starts and runs the RedshiftBasicExecutor.
     * 
     * @param args
     */
    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not load PostgreSQL driver");
        }
        KinesisConnectorExecutor<KinesisMessageModel, byte[]> redshiftExecutor = new RedshiftBasicExecutor(CONFIG_FILE);
        redshiftExecutor.run();
    }
}
