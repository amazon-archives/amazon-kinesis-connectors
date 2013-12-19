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
package com.amazonaws.services.kinesis.connectors;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;

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
    public KinesisConnectorRecordProcessor<T, U> createProcessor() {
        try {
            IBuffer<T> buffer = pipeline.getBuffer(configuration);
            IEmitter<U> emitter = pipeline.getEmitter(configuration);
            ITransformer<T, U> transformer = pipeline.getTransformer(configuration);
            IFilter<T> filter = pipeline.getFilter(configuration);
            KinesisConnectorRecordProcessor<T, U> processor = new KinesisConnectorRecordProcessor<T, U>(
                    buffer, filter, emitter, transformer, configuration);
            return processor;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
