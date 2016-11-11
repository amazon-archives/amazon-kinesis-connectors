/*
 * Copyright 2013-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.connectors;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;

/**
 * Wraps an {@link IRecordProcessorFactory} to decouple fetching records and processing records in the created {@link IRecordProcessor}. The
 * {@link IRecordProcessor} must checkpoint using the {@link IRecordProcessorCheckpointer#checkpoint(String)} method with a specific sequence number.
 * Checkpointing using {@link IRecordProcessorCheckpointer#checkpoint()} may only be used in
 * {@link IRecordProcessor#shutdown(IRecordProcessorCheckpointer, ShutdownReason)}. Using {@link IRecordProcessorCheckpointer#checkpoint()} in processRecords
 * will result in an {@link UnsupportedOperationException}.
 */
public class KinesisClientLibraryPipelinedRecordProcessorFactory implements IRecordProcessorFactory {
    /**
     * The wrapped record processor factory.
     */
    private final IRecordProcessorFactory recordProcessorFactory;
    /**
     * The maximum number of records to retrieve and buffer in memory.
     */
    private final int maxQueueSize;
    /**
     *
     */
    private final Long maxQueueWaitTimeMs;
    /**
     *
     */
    private final Long maxProcessRecordsWaitTimeMs;

    /**
     * Constructor to wrap an {@link IRecordProcessorFactory} as a pipelined record processor factory. Default values are used for maximum queue wait time and
     * maximum process records wait time.
     *
     * @param factory
     *            The {@link IRecordProcessorFactory} to wrap
     * @param maxQueueSize
     *            The maximum number of records to retrieve and buffer in memory
     */
    public KinesisClientLibraryPipelinedRecordProcessorFactory(IRecordProcessorFactory factory, int maxQueueSize) {
        this.recordProcessorFactory = factory;
        this.maxQueueSize = maxQueueSize;
        this.maxQueueWaitTimeMs = null;
        this.maxProcessRecordsWaitTimeMs = null;
    }

    /**
     * Constructor to wrap an {@link IRecordProcessorFactory} as a pipelined record processor factory. If null values are passed for maxQueueWaitTimeMs or
     * maxProcessRecordsWaitTimeMs, default values are used.
     *
     * @param factory
     *            The {@link IRecordProcessorFactory} to wrap
     * @param maxQueueSize
     *            The maximum number of records to retrieve and buffer in memory
     * @param maxQueueWaitTimeMs
     *            Maximum time to block on the queue waiting for GetRecords result in milliseconds
     * @param maxProcessRecordsWaitTimeMs
     *            Maximum time to wait for the queue consumer to shutdown (finish ProcessRecords call) in milliseconds
     */
    public KinesisClientLibraryPipelinedRecordProcessorFactory(IRecordProcessorFactory factory, int maxQueueSize, Long maxQueueWaitTimeMs,
        Long maxProcessRecordsWaitTimeMs) {
        this.recordProcessorFactory = factory;
        this.maxQueueSize = maxQueueSize;
        this.maxQueueWaitTimeMs = maxQueueWaitTimeMs;
        this.maxProcessRecordsWaitTimeMs = maxProcessRecordsWaitTimeMs;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisClientLibraryPipelinedRecordProcessor(recordProcessorFactory.createProcessor(), maxQueueSize, maxQueueWaitTimeMs,
            maxProcessRecordsWaitTimeMs);
    }

}
