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

import java.util.List;

/**
 * IBuffer defines a buffer used to store records streamed through Amazon Kinesis. It is a part of the
 * IKinesisConnectorPipeline utilized by the KinesisConnectorRecordProcessor. Records are stored in
 * the buffer by calling the consumeRecord method. The buffer has two size limits defined: total
 * byte count and total number of records. The shouldFlush() method may indicate that the buffer is
 * full based on these limits.
 * 
 * @param <T>
 *        the data type stored in the record
 */
public interface IBuffer<T> {
    /**
     * Get the byte size limit of data stored in the buffer before the records are flushed to the
     * emitter
     * 
     * @return byte size limit of buffer
     */
    public long getBytesToBuffer();

    /**
     * Get the record number limit of data stored in the buffer before the records are flushed to
     * the emitter
     * 
     * @return record number limit of buffer
     */
    public long getNumRecordsToBuffer();

    /**
     * Get the time limit in milliseconds before the records are flushed to the emitter
     * 
     * @return time limit in milleseconds
     */
    public long getMillisecondsToBuffer();

    /**
     * Returns true if the buffer is full and stored records should be sent to the emitter
     * 
     * @return true if records should be sent to the emitter followed by clearing the buffer
     */
    public boolean shouldFlush();

    /**
     * Stores the record in the buffer
     * 
     * @param record
     *        record to be processed
     * @param recordBytes
     *        size of the record data in bytes
     * @param sequenceNumber
     *        Amazon Kinesis sequence identifier
     */
    public void consumeRecord(T record, int recordBytes, String sequenceNumber);

    /**
     * Clears the buffer
     */
    public void clear();

    /**
     * Get the sequence number of the first record stored in the buffer. Used for bookkeeping and
     * uniquely identifying items in the buffer.
     * 
     * @return the sequence number of the first record stored in the buffer
     */
    public String getFirstSequenceNumber();

    /**
     * Get the sequence number of the last record stored in the buffer. Used for bookkeeping and
     * uniquely identifying items in the buffer.
     * 
     * @return the sequence number of the last record stored in the buffer
     */
    public String getLastSequenceNumber();

    /**
     * Get the records stored in the buffer
     * 
     * @return the records stored in the buffer
     */
    public List<T> getRecords();
}
