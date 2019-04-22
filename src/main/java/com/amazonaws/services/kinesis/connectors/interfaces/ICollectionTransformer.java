/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.connectors.interfaces;

import java.io.IOException;
import java.util.Collection;

import com.amazonaws.services.kinesis.model.Record;

/**
 * ICollectionTransformer is used to transform data from an Amazon Kinesis Record (byte array) to
 * a collection of the data model class (T) for processing in the application and from the
 * data model class to the output type (U) for the emitter.
 * 
 * @param <T>
 *        the data type stored in the record
 * @param <U>
 *        the data type to emit
 */
public interface ICollectionTransformer<T, U> extends ITransformerBase<T, U> {
    /**
     * Transform record into a collection of object of their original class.
     * 
     * @param record
     *        raw record from the Amazon Kinesis stream
     * @return data as its original class
     * @throws IOException
     *         could not convert the record to a Collection<T>
     */
    public Collection<T> toClass(Record record) throws IOException;
}
