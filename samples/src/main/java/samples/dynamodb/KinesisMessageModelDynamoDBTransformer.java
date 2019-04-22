/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.dynamodb;

import java.util.HashMap;
import java.util.Map;

import samples.KinesisMessageModel;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.BasicJsonTransformer;
import com.amazonaws.services.kinesis.connectors.dynamodb.DynamoDBTransformer;

/**
 * A custom transfomer for {@link KinesisMessageModel} records in JSON format. The output is in a format
 * usable for insertions to Amazon DynamoDB.
 */
public class KinesisMessageModelDynamoDBTransformer extends
        BasicJsonTransformer<KinesisMessageModel, Map<String, AttributeValue>> implements
        DynamoDBTransformer<KinesisMessageModel> {

    /**
     * Creates a new KinesisMessageModelDynamoDBTransformer.
     */
    public KinesisMessageModelDynamoDBTransformer() {
        super(KinesisMessageModel.class);
    }

    @Override
    public Map<String, AttributeValue> fromClass(KinesisMessageModel message) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        putIntegerIfNonempty(item, "user_id", message.userid);
        putStringIfNonempty(item, "username", message.username);
        putStringIfNonempty(item, "firstname", message.firstname);
        putStringIfNonempty(item, "lastname", message.lastname);
        putStringIfNonempty(item, "city", message.city);
        putStringIfNonempty(item, "state", message.state);
        putStringIfNonempty(item, "email", message.email);
        putStringIfNonempty(item, "phone", message.phone);
        putBoolIfNonempty(item, "likesports", message.likesports);
        putBoolIfNonempty(item, "liketheatre", message.liketheatre);
        putBoolIfNonempty(item, "likeconcerts", message.likeconcerts);
        putBoolIfNonempty(item, "likejazz", message.likejazz);
        putBoolIfNonempty(item, "likeclassical", message.likeclassical);
        putBoolIfNonempty(item, "likeopera", message.likeopera);
        putBoolIfNonempty(item, "likerock", message.likerock);
        putBoolIfNonempty(item, "likevegas", message.likevegas);
        putBoolIfNonempty(item, "likebroadway", message.likebroadway);
        putBoolIfNonempty(item, "likemusicals", message.likemusicals);
        return item;
    }

    /**
     * Helper method to map nonempty String attributes to an AttributeValue.
     * 
     * @param item The map of attribute names to AttributeValues to store the attribute in
     * @param key The key to store in the map
     * @param value The value to check before inserting into the item map
     */
    private void putStringIfNonempty(Map<String, AttributeValue> item, String key, String value) {
        if (value != null && !value.isEmpty()) {
            item.put(key, new AttributeValue().withS(value));
        }
    }

    /**
     * Helper method to map boolean attributes to an AttributeValue.
     * 
     * @param item The map of attribute names to AttributeValues to store the attribute in
     * @param key The key to store in the map
     * @param value The value to insert into the item map
     */
    private void putBoolIfNonempty(Map<String, AttributeValue> item, String key, Boolean value) {
        putStringIfNonempty(item, key, Boolean.toString(value));
    }

    /**
     * Helper method to map nonempty Integer attributes to an AttributeValue.
     * 
     * @param item The map of attribute names to AttributeValues to store the attribute in
     * @param key The key to store in the map
     * @param value The value to insert into the item map
     */
    private void putIntegerIfNonempty(Map<String, AttributeValue> item, String key, Integer value) {
        putStringIfNonempty(item, key, Integer.toString(value));
    }
}
