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
package com.amazonaws.services.kinesis.connectors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.ICollectionTransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase;
import com.amazonaws.services.kinesis.model.Record;

/**
 * This is the base class for any KinesisConnector. It is configured by a constructor that takes in
 * as parameters implementations of the IBuffer, ITransformer, and IEmitter dependencies defined in
 * a IKinesisConnectorPipeline. It is typed to match the class that records are transformed into for
 * filtering and manipulation. This class is produced by a KinesisConnectorRecordProcessorFactory.
 * <p>
 * When a Worker calls processRecords() on this class, the pipeline is used in the following way:
 * <ol>
 * <li>Records are transformed into the corresponding data model (parameter type T) via the ITransformer.</li>
 * <li>Transformed records are passed to the IBuffer.consumeRecord() method, which may optionally filter based on the
 * IFilter in the pipeline.</li>
 * <li>When the buffer is full (IBuffer.shouldFlush() returns true), records are transformed with the ITransformer to
 * the output type (parameter type U) and a call is made to IEmitter.emit(). IEmitter.emit() returning an empty list is
 * considered a success, so the record processor will checkpoint and emit will not be retried. Non-empty return values
 * will result in additional calls to emit with failed records as the unprocessed list until the retry limit is reached.
 * Upon exceeding the retry limit or an exception being thrown, the IEmitter.fail() method will be called with the
 * unprocessed records.</li>
 * <li>When the shutdown() method of this class is invoked, a call is made to the IEmitter.shutdown() method which
 * should close any existing client connections.</li>
 * </ol>
 *
 */
public class KinesisConnectorRecordProcessor<T, U> implements IRecordProcessor {

    private final IEmitter<U> emitter;
    private final ITransformerBase<T, U> transformer;
    private final IFilter<T> filter;
    private final IBuffer<T> buffer;
    private final int retryLimit;
    private final long backoffInterval;
    private boolean isShutdown = false;

    private static final Log LOG = LogFactory.getLog(KinesisConnectorRecordProcessor.class);

    private String shardId;

    public KinesisConnectorRecordProcessor(IBuffer<T> buffer,
            IFilter<T> filter,
            IEmitter<U> emitter,
            ITransformerBase<T, U> transformer,
            KinesisConnectorConfiguration configuration) {
        if (buffer == null || filter == null || emitter == null || transformer == null) {
            throw new IllegalArgumentException("buffer, filter, emitter, and transformer must not be null");
        }
        this.buffer = buffer;
        this.filter = filter;
        this.emitter = emitter;
        this.transformer = transformer;
        // Limit must be greater than zero
        if (configuration.RETRY_LIMIT <= 0) {
            retryLimit = 1;
        } else {
            retryLimit = configuration.RETRY_LIMIT;
        }
        this.backoffInterval = configuration.BACKOFF_INTERVAL;
    }

    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        // Note: This method will be called even for empty record lists. This is needed for checking the buffer time
        // threshold.
        if (isShutdown) {
            LOG.warn("processRecords called on shutdown record processor for shardId: " + shardId);
            return;
        }
        if (shardId == null) {
            throw new IllegalStateException("Record processor not initialized");
        }

        // Transform each Amazon Kinesis Record and add the result to the buffer
        for (Record record : records) {
            try {
                if (transformer instanceof ITransformer) {
                    ITransformer<T, U> singleTransformer = (ITransformer<T, U>) transformer;
                    filterAndBufferRecord(singleTransformer.toClass(record), record);
                } else if (transformer instanceof ICollectionTransformer) {
                    ICollectionTransformer<T, U> listTransformer = (ICollectionTransformer<T, U>) transformer;
                    Collection<T> transformedRecords = listTransformer.toClass(record);
                    for (T transformedRecord : transformedRecords) {
                        filterAndBufferRecord(transformedRecord, record);
                    }
                } else {
                    throw new RuntimeException("Transformer must implement ITransformer or ICollectionTransformer");
                }
            } catch (IOException e) {
                LOG.error(e);
            }
        }

        if (buffer.shouldFlush()) {
            List<U> emitItems = transformToOutput(buffer.getRecords());
            emit(checkpointer, emitItems);
        }
    }

    private void filterAndBufferRecord(T transformedRecord, Record record) {
        if (filter.keepRecord(transformedRecord)) {
            buffer.consumeRecord(transformedRecord, record.getData().array().length, record.getSequenceNumber());
        }
    }

    private List<U> transformToOutput(List<T> items) {
        List<U> emitItems = new ArrayList<U>();
        for (T item : items) {
            try {
                emitItems.add(transformer.fromClass(item));
            } catch (IOException e) {
                LOG.error("Failed to transform record " + item + " to output type", e);
            }
        }
        return emitItems;
    }

    private void emit(IRecordProcessorCheckpointer checkpointer, List<U> emitItems) {
        List<U> unprocessed = new ArrayList<U>(emitItems);
        try {
            for (int numTries = 0; numTries < retryLimit; numTries++) {
                unprocessed = emitter.emit(new UnmodifiableBuffer<U>(buffer, unprocessed));
                if (unprocessed.isEmpty()) {
                    break;
                }
                try {
                    Thread.sleep(backoffInterval);
                } catch (InterruptedException e) {
                }
            }
            if (!unprocessed.isEmpty()) {
                emitter.fail(unprocessed);
            }
            final String lastSequenceNumberProcessed = buffer.getLastSequenceNumber();
            buffer.clear();
            // checkpoint once all the records have been consumed
            if (lastSequenceNumberProcessed != null) {
                checkpointer.checkpoint(lastSequenceNumberProcessed);
            }
        } catch (IOException | KinesisClientLibDependencyException | InvalidStateException | ThrottlingException
                | ShutdownException e) {
            LOG.error(e);
            emitter.fail(unprocessed);
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor with shardId: " + shardId + " with reason " + reason);
        if (isShutdown) {
            LOG.warn("Record processor for shardId: " + shardId + " has been shutdown multiple times.");
            return;
        }
        switch (reason) {
            case TERMINATE:
                emit(checkpointer, transformToOutput(buffer.getRecords()));
                try {
                    checkpointer.checkpoint();
                } catch (KinesisClientLibDependencyException | InvalidStateException | ThrottlingException | ShutdownException e) {
                    LOG.error(e);
                }
                break;
            case ZOMBIE:
                break;
            default:
                throw new IllegalStateException("invalid shutdown reason");
        }
        emitter.shutdown();
        isShutdown = true;
    }

}
