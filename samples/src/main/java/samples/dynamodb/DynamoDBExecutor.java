/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
