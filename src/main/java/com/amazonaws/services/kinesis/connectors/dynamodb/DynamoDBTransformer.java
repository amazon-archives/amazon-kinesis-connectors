/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.connectors.dynamodb;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;

/**
 * This interface defines an ITransformer that has an output type of Map of attribute name (String)
 * to AttributeValue so that the item can be put into Amazon DynamoDB.
 * 
 * @param <T>
 */
public interface DynamoDBTransformer<T> extends ITransformer<T, Map<String, AttributeValue>> {

}
