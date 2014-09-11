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

import samples.KinesisMessageModel;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.dynamodb.DynamoDBEmitter;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;

/**
 * The Pipeline used by the Amazon DynamoDB sample. Processes KinesisMessageModel records in JSON String
 * format. Uses:
 * <ul>
 * <li>{@link DynamoDBEmitter}</li>
 * <li>{@link BasicMemoryBuffer}</li>
 * <li>{@link KinesisMessageModelDynamoDBTransformer}</li>
 * <li>{@link AllPassFilter}</li>
 * </ul>
 */
public class DynamoDBMessageModelPipeline implements
        IKinesisConnectorPipeline<KinesisMessageModel, Map<String, AttributeValue>> {

    @Override
    public IEmitter<Map<String, AttributeValue>> getEmitter(KinesisConnectorConfiguration configuration) {
        return new DynamoDBEmitter(configuration);
    }

    @Override
    public IBuffer<KinesisMessageModel> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<KinesisMessageModel>(configuration);
    }

    @Override
    public ITransformer<KinesisMessageModel, Map<String, AttributeValue>>
            getTransformer(KinesisConnectorConfiguration configuration) {
        return new KinesisMessageModelDynamoDBTransformer();
    }

    @Override
    public IFilter<KinesisMessageModel> getFilter(KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<KinesisMessageModel>();
    }

}
