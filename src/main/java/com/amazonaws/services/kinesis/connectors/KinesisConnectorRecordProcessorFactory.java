/*
 * Copyright 2013-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.connectors;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase;

/**
 * This class is used to generate KinesisConnectorRecordProcessors that operate using the user's
 * implemented classes. The createProcessor() method sets the dependencies of the
 * KinesisConnectorRecordProcessor that are specified in the KinesisConnectorPipeline argument,
 * which accesses instances of the users implementations.
 */
public class KinesisConnectorRecordProcessorFactory<T, U> implements IRecordProcessorFactory {

    private IKinesisConnectorPipeline<T, U> pipeline;
    private KinesisConnectorConfiguration configuration;

    public KinesisConnectorRecordProcessorFactory(IKinesisConnectorPipeline<T, U> pipeline,
            KinesisConnectorConfiguration configuration) {
        this.configuration = configuration;
        this.pipeline = pipeline;
    }

    @Override
    public IRecordProcessor createProcessor() {
        try {
            IBuffer<T> buffer = pipeline.getBuffer(configuration);
            IEmitter<U> emitter = pipeline.getEmitter(configuration);
            ITransformerBase<T, U> transformer = pipeline.getTransformer(configuration);
            IFilter<T> filter = pipeline.getFilter(configuration);
            KinesisConnectorRecordProcessor<T, U> processor =
                    new KinesisConnectorRecordProcessor<T, U>(buffer, filter, emitter, transformer, configuration);
            return processor;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
