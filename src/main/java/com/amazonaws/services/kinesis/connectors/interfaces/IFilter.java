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
