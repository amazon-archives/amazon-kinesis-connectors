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

/**
 * Base transformer class to provide backwards compatibility with ITransformer
 * while supporting the ICollectionTransformer.
 * 
 * This class is not meant to be implemented. Instead, implement ITransformer
 * or ICollectionTransformer depending on the type of PutRecordRequests you are
 * doing for proper toClass transforms.
 * - Use ITransformer if each Amazon Kinesis Record contains one object of type T.
 * - Use ICollectionTransformer if each Amazon Kinesis Record contains a Collection<T>
 * (batched PutRecordRequests).
 * 
 * @param <T>
 *        the data type stored in the record
 * @param <U>
 *        the data type to emit
 */
public abstract interface ITransformerBase<T, U> {

    /**
     * Transform record from its original class to final output class.
     * 
     * @param record
     *        data as its original class
     * @return U
     *         the object as its final class
     */
    public U fromClass(T record) throws IOException;
}
