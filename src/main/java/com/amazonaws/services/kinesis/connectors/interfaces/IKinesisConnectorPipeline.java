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

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;

/**
 * This interface is used by the KinesisConnectorRecordProcessorFactory to obtain instances of the
 * user's implemented classes. Each method takes the applications configuration as an argument. The
 * user should implement this such that each method returns a configured implementation of each
 * interface. It has two parameter types, the data input type (T) and the data output type (U).
 * Records come in as a byte[] and are transformed to a T. Then they are buffered in T form. When
 * the buffer is full, T's are converted to U's and passed to the emitter.
 * 
 */
public interface IKinesisConnectorPipeline<T, U> {
    /**
     * Return an instance of the users implementation of IEmitter
     * 
     * @param configuration
     * @return a configured instance of the IEmitter implementation.
     */
    IEmitter<U> getEmitter(KinesisConnectorConfiguration configuration);

    /**
     * Return an instance of the users implementation of IBuffer
     * 
     * @param configuration
     * @return a configured instance of the IBuffer implementation.
     */
    IBuffer<T> getBuffer(KinesisConnectorConfiguration configuration);

    /**
     * Return an instance of the users implementation of ITransformer.
     * 
     * @param configuration
     * @return a configured instance of the ITransformer implementation
     */
    ITransformerBase<T, U> getTransformer(KinesisConnectorConfiguration configuration);

    /**
     * Return an instance of the users implementation of IFilter.
     * 
     * @param configuration
     * @return a configured instance of the IFilter implementation.
     */
    IFilter<T> getFilter(KinesisConnectorConfiguration configuration);
}
