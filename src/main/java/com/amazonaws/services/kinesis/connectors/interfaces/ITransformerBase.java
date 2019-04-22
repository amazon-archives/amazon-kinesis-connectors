/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
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
