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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

/**
 * <p>
 * Wraps an {@link IRecordProcessor} to pipeline fetching and processing records.
 * </p>
 * <p>
 * This decouples reading from a stream and processing the records retrieved from the stream. This is useful when either GetRecords or ProcessRecords calls take
 * significant time (e.g. cross-region calls, long-lived tasks, etc). By separating the functionality, the total throughput of the application becomes limited
 * by max(time(GetRecords), time(ProcessRecords)) rather than time(GetRecords) + time(ProcessRecords).
 * </p>
 * <p>
 * The processRecords method fills a bounded-size, blocking queue with the records to bound the amount of memory required for the record processor. A separate
 * thread consumes the queue and passes batches of records to the wrapped {@link IRecordProcessor}.
 * </p>
 * <p>
 * The {@link IRecordProcessor} must checkpoint using the {@link IRecordProcessorCheckpointer#checkpoint(String)} method with a specific sequence number.
 * Checkpointing using {@link IRecordProcessorCheckpointer#checkpoint()} may only be used in
 * {@link IRecordProcessor#shutdown(IRecordProcessorCheckpointer, ShutdownReason)}. Using {@link IRecordProcessorCheckpointer#checkpoint()} in processRecords
 * will result in an {@link UnsupportedOperationException}.
 * </p>
 */
public class KinesisClientLibraryPipelinedRecordProcessor implements IRecordProcessor {
    /**
     * Default maximum time to block on the queue waiting for GetRecords result.
     */
    public static final long DEFAULT_MAXIMUM_QUEUE_WAIT_TIME_MS = 5000;
    /**
     * Default maximum time to wait for the queue consumer to shutdown (finish ProcessRecords call).
     */
    public static final long DEFAULT_MAXIMUM_PROCESS_RECORDS_WAIT_TIME_MS = 60000;
    /**
     * Class logger.
     */
    private static final Log LOG = LogFactory.getLog(KinesisClientLibraryPipelinedRecordProcessor.class);
    /**
     * Maximum time to block on the queue waiting for GetRecords result in milliseconds.
     */
    private final long maxQueueWaitTimeMs;
    /**
     * Maximum time to wait for the queue consumer to shutdown (finish ProcessRecords call) in milliseconds.
     */
    private final long maxProcessRecordsWaitTimeMs;
    /**
     * Queue for the records.
     */
    private final BlockingQueue<Record> recordQueue;
    /**
     * The wrapped record processor.
     */
    private final IRecordProcessor recordProcessor;
    /**
     * Executor service for running the queue consumer.
     */
    private final ExecutorService queueConsumerExecutor = Executors.newSingleThreadExecutor();
    /**
     * The queue consumer runnable.
     */
    private QueueConsumer queueConsumer;
    /**
     * The shard being processed.
     */
    private String shardId;

    /**
     * Constructor. Default values are used for maximum queue wait time and maximum process records wait time.
     *
     * @param recordProcessor
     *            The record processor to wrap
     * @param maxQueueSize
     *            The maximum queue size
     */
    public KinesisClientLibraryPipelinedRecordProcessor(IRecordProcessor recordProcessor, int maxQueueSize) {
        this(recordProcessor, maxQueueSize, DEFAULT_MAXIMUM_QUEUE_WAIT_TIME_MS, DEFAULT_MAXIMUM_PROCESS_RECORDS_WAIT_TIME_MS);
    }

    /**
     * Constructor. If null values are provided for maxQueueWaitTimeMs and/or maxProcessRecordsWaitTimeMs, default values are used.
     *
     * @param recordProcessor
     *            The record processor to wrap
     * @param maxQueueSize
     *            The maximum queue size
     * @param maxQueueWaitTimeMs
     *            Maximum time to block on the queue waiting for GetRecords result in milliseconds
     * @param maxProcessRecordsWaitTimeMs
     *            Maximum time to wait for the queue consumer to shutdown (finish ProcessRecords call) in milliseconds
     */
    public KinesisClientLibraryPipelinedRecordProcessor(IRecordProcessor recordProcessor, int maxQueueSize, Long maxQueueWaitTimeMs,
        Long maxProcessRecordsWaitTimeMs) {
        this.recordProcessor = recordProcessor;
        recordQueue = new LinkedBlockingQueue<Record>(maxQueueSize);
        this.maxQueueWaitTimeMs = (maxQueueWaitTimeMs == null) ? DEFAULT_MAXIMUM_QUEUE_WAIT_TIME_MS : maxQueueWaitTimeMs;
        this.maxProcessRecordsWaitTimeMs = (maxProcessRecordsWaitTimeMs == null) ? DEFAULT_MAXIMUM_PROCESS_RECORDS_WAIT_TIME_MS : maxProcessRecordsWaitTimeMs;
    }

    @Override
    public void initialize(String shardId) {
        if (shardId == null) {
            throw new IllegalArgumentException("ShardId cannot be null");
        }
        this.shardId = shardId;
        recordProcessor.initialize(shardId);
        queueConsumer = new QueueConsumer();
        queueConsumerExecutor.submit(queueConsumer);
        queueConsumerExecutor.shutdown();
        LOG.info("Initialized pipelined record processor for shard: " + shardId);
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        queueConsumer.setCheckpointer(checkpointer);
        for (Record record : records) {
            try {
                recordQueue.put(record);
            } catch (InterruptedException e) {
                LOG.error("Interrupted while adding a record to the queue", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down pipelined processor for shard: " + shardId + " with reason:" + reason);
        queueConsumer.shutdown = true;
        try {
            if (queueConsumerExecutor.awaitTermination(maxProcessRecordsWaitTimeMs, TimeUnit.MILLISECONDS)) {
                List<Record> records = new ArrayList<Record>();
                recordQueue.drainTo(records);
                // No need to protect the checkpointer any longer. Record processing is in sync with record fetching.
                recordProcessor.processRecords(records, checkpointer);
                recordProcessor.shutdown(checkpointer, reason);
            } else {
                LOG.warn("Queue consumer took longer than " + maxProcessRecordsWaitTimeMs + " ms to complete. Shutdown task failed.");
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while draining queue", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Asynchronous queue consumer to process records.
     */
    private class QueueConsumer implements Runnable {
        /**
         * Flag to shutdown the queue consumer.
         */
        volatile boolean shutdown = false;
        /**
         * The latest checkpointer. Is wrapped to protect user from calling default checkpoint method. All access should be synchronized on the instance of
         * {@link QueueConsumer}.
         */
        private volatile IRecordProcessorCheckpointer checkpointer = null;

        public void setCheckpointer(IRecordProcessorCheckpointer checkpointer) {
            this.checkpointer = protectCheckpointer(checkpointer);
        }

        @Override
        public void run() {
            LOG.info("Starting queue consumer for shard: " + shardId);
            while (!shutdown) {
                consumeQueue();
            }
            LOG.info("Queue consumer terminated for shard: " + shardId);
        }

        /**
         * Processes the records in the queue using the wrapped {@link IRecordProcessor}.
         */
        private void consumeQueue() {
            final List<Record> records = new ArrayList<Record>();
            int drained = 0;
            // Use blocking queue's poll with timeout to wait for new records
            Record polled = null;
            try {
                polled = recordQueue.poll(maxQueueWaitTimeMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.error(e);
                Thread.currentThread().interrupt();
            }
            // Check if queue contained records.
            if (polled == null) {
                processRecords(records /* Empty list */, checkpointer);
                return;
            }
            records.add(polled);
            drained++;
            // Drain the remaining of the records
            drained += recordQueue.drainTo(records);
            recordProcessor.processRecords(records, checkpointer /* Protected checkpointer */);
            LOG.info("Consumed " + drained + " records");
        }
    }

    /**
     * Wraps a checkpointer to prevent users from using the default checkpoint method. Decoupling record retrieval and processing means the checkpointer
     * sequence number may no longer be accurate.
     *
     * @param checkpointer
     *            The checkpointer to protect
     * @return A wrapped checkpointer that does not allow the default checkpoint method.
     */
    IRecordProcessorCheckpointer protectCheckpointer(final IRecordProcessorCheckpointer checkpointer) {
        return new IRecordProcessorCheckpointer() {
            /**
             * Protected checkpointer.
             */
            private final IRecordProcessorCheckpointer internalCheckpointer = checkpointer;

            @Override
            public void checkpoint(String sequenceNumber) throws KinesisClientLibDependencyException,
                    InvalidStateException, ThrottlingException, ShutdownException, IllegalArgumentException {
                internalCheckpointer.checkpoint(sequenceNumber);
            }

            @Override
            public void checkpoint() throws KinesisClientLibDependencyException,
                    InvalidStateException, ThrottlingException, ShutdownException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void checkpoint(Record record) throws KinesisClientLibDependencyException,
                    InvalidStateException, ThrottlingException, ShutdownException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void checkpoint(String sequenceNumber, long subSequenceNumber) throws KinesisClientLibDependencyException,
                    InvalidStateException, ThrottlingException, ShutdownException, IllegalArgumentException {
                throw new UnsupportedOperationException();
            }
        };
    }
}
