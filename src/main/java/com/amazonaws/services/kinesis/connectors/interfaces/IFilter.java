/*
 * Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.connectors.interfaces;

/**
 * The IFilter is associated with an IBuffer. The IBuffer may use the result of calling the
 * keepRecord() method to decide whether to store a record or discard it.
 * 
 * @param <T>
 *        the data type stored in the record
 */
public interface IFilter<T> {

    /**
     * A method enabling the buffer to filter records. Return false if you don't want to hold on to
     * the record.
     * 
     * @param record
     * @return true if the record should be added to the buffer.
     */
    public boolean keepRecord(T record);

}
