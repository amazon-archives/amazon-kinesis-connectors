/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.connectors.redshift;

import com.amazonaws.services.kinesis.connectors.impl.JsonToByteArrayTransformer;

/**
 * This class is an implementation of the ITransformer interface and an extension of the
 * BasicJsonTransformer class. The abstract method toDelimitedString() requires implementing classes
 * to output a delimited-string representation of the data model that is compatible with an
 * insertion into Amazon Redshift.
 * 
 * @param <T>
 */
public abstract class RedshiftTransformer<T> extends JsonToByteArrayTransformer<T> {

    public RedshiftTransformer(Class<T> clazz) {
        super(clazz);
    }

    /**
     * This method requires implementing classes to output a string representation of the data model
     * that is compatible with Amazon Redshift. This string will be used to insert records into an Amazon Redshift
     * table, and should be in a delimited format.
     * 
     * @param recordObject
     *        the instance of the data model to convert to delimited string.
     * @return a delimited string representation of the data model that is compatible with Amazon Redshift
     */
    public abstract String toDelimitedString(T recordObject);

    @Override
    public byte[] fromClass(T record) {
        return toDelimitedString(record).getBytes();
    }

}
