/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.connectors.dynamodb;

import static junit.framework.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.dynamodb.DynamoDBEmitter;

public class DynamoDBEmitterTests {
    IMocksControl control;
    KinesisConnectorConfiguration config;
    AWSCredentialsProvider credsProvider;

    @Before
    public void setUp() {
        control = EasyMock.createControl();
        // mock properties
        Properties props = new Properties();

        // Mock a redentials provider for constructor arg
        credsProvider = control.createMock(AWSCredentialsProvider.class);
        config = new KinesisConnectorConfiguration(props, credsProvider);
    }

    @Test
    public void testNoDuplicates() {
        int listSize = 100;
        List<Map<String, AttributeValue>> listWithDuplicates = createDuplicateList(listSize);
        Set<Map<String, AttributeValue>> listWithoutDuplicates = new HashSet<Map<String, AttributeValue>>();
        control.reset();
        control.replay();
        DynamoDBEmitter emitter = new DynamoDBEmitter(config);

        listWithoutDuplicates = emitter.uniqueItems(listWithDuplicates);
        assertEquals(1, listWithoutDuplicates.size());

    }

    private List<Map<String, AttributeValue>> createDuplicateList(int size) {
        List<Map<String, AttributeValue>> list = new ArrayList<Map<String, AttributeValue>>();

        for (int i = 0; i < size; i++) {
            String itemName = "item";
            AttributeValue itemValue = new AttributeValue().withS(itemName);
            Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
            item.put(itemName, itemValue);
            list.add(item);
        }

        return list;
    }

}
