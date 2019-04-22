/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.dynamodb;

import java.util.Map;

import samples.KinesisConnectorExecutor;
import samples.KinesisMessageModel;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;

/**
 * The Executor for the Amazon DynamoDB emitter sample.
 */
public class DynamoDBExecutor extends KinesisConnectorExecutor<KinesisMessageModel, Map<String, AttributeValue>> {

    private static String configFile = "DynamoDBSample.properties";

    /**
     * Creates a new DynamoDBExcecutor.
     * 
     * @param configFile The name of the configuration file to look for on the classpath.
     */
    public DynamoDBExecutor(String configFile) {
        super(configFile);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<KinesisMessageModel, Map<String, AttributeValue>>
            getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<KinesisMessageModel, Map<String, AttributeValue>>(new DynamoDBMessageModelPipeline(),
                config);
    }

    /**
     * Main method starts and runs the DynamoDBExecutor.
     * 
     * @param args
     */
    public static void main(String[] args) {
        KinesisConnectorExecutor<KinesisMessageModel, Map<String, AttributeValue>> dynamoDBExecutor =
                new DynamoDBExecutor(configFile);
        dynamoDBExecutor.run();
    }
}
