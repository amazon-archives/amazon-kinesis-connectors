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
