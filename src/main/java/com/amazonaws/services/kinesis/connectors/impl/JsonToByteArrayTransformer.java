/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.connectors.impl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.connectors.BasicJsonTransformer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The JsonToByteArrayTransformer defines a BasicJsonTransformer with byte array for its output
 * type. This allows for data to be sent to Amazon S3 or an Amazon Kinesis stream.
 */
public class JsonToByteArrayTransformer<T> extends BasicJsonTransformer<T, byte[]> {
    private static final Log LOG = LogFactory.getLog(JsonToByteArrayTransformer.class);

    public JsonToByteArrayTransformer(Class<T> inputClass) {
        super(inputClass);
    }

    @Override
    public byte[] fromClass(T record) throws IOException {
        try {
            return new ObjectMapper().writeValueAsString(record).getBytes();
        } catch (JsonProcessingException e) {
            String message = "Error parsing record to JSON";
            LOG.error(message, e);
            throw new IOException(message, e);
        }

    }

}
