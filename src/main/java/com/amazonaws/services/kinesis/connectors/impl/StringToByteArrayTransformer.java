/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.connectors.impl;

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.model.Record;

/**
 * This class is an implementation of the ITransformer to transform between raw Amazon Kinesis records and
 * strings. It assumes that the Record parameter of toClass() is a byte array representation of a
 * string.
 * 
 */
public class StringToByteArrayTransformer implements ITransformer<String, byte[]> {

    @Override
    public String toClass(Record record) {
        return new String(record.getData().array());
    }

    @Override
    public byte[] fromClass(String record) {
        return record.getBytes();
    }
}
