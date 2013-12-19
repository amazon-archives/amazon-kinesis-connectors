/*
 * Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.connectors.redshift;

import com.amazonaws.services.kinesis.connectors.impl.JsonToByteArrayTransformer;

/**
 * This class is an implementation of the ITransformer interface and an extension of the
 * BasicJsonTransformer class. The abstract method toDelimitedString() requires implementing classes
 * to output a delimited-string representation of the data model that is compatible with an
 * insertion into Redshift.
 * 
 * @param <T>
 */
public abstract class RedshiftTransformer<T> extends JsonToByteArrayTransformer<T> {

    public RedshiftTransformer(Class<T> clazz) {
        super(clazz);
    }

    /**
     * This method requires implementing classes to output a string representation of the data model
     * that is compatible with Redshift. This string will be used to insert records into a Redshift
     * table, and should be in a delimited format.
     * 
     * @param recordObject
     *            the instance of the data model to convert to delimited string.
     * @return a delimited string representation of the data model that is compatible with Redshift
     */
    public abstract String toDelimitedString(T recordObject);

    @Override
    public byte[] fromClass(T record) {
        return toDelimitedString(record).getBytes();
    }

}