/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.connectors.interfaces;

import java.io.IOException;
import java.util.List;

import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;

/**
 * IEmitter takes a full buffer and processes the stored records. The IEmitter is a member of the
 * IKinesisConnectorPipeline that "emits" the objects that have been deserialized by the
 * ITransformer. The emit() method is invoked when the buffer is full (possibly to persist the
 * records or send them to another Amazon Kinesis stream). After emitting the records, the IEmitter should
 * return a list of records that could not be processed. Implementations may choose to fail the
 * entire set of records in the buffer or to fail records individually.
 * 
 * @param <T>
 *        the data type stored in the record
 */
public interface IEmitter<T> {

    /**
     * Invoked when the buffer is full. This method emits the set of filtered records. It should
     * return a list of records that were not emitted successfully. Returning
     * Collections.emptyList() is considered a success.
     * 
     * @param buffer
     *        The full buffer of records
     * @throws IOException
     *         A failure was reached that is not recoverable, no retry will occur and the fail
     *         method will be called
     * @return A list of records that failed to emit to be retried
     */
    List<T> emit(UnmodifiableBuffer<T> buffer) throws IOException;

    /**
     * This method defines how to handle a set of records that cannot successfully be emitted.
     * 
     * @param records
     *        a list of records that were not successfully emitted
     */
    void fail(List<T> records);

    /**
     * This method is called when the KinesisConnectorRecordProcessor is shutdown. It should close
     * any existing client connections.
     */
    void shutdown();
}
