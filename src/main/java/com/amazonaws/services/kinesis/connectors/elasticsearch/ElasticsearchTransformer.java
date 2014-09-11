/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.connectors.elasticsearch;

import java.io.IOException;

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase;

/**
 * Transformer to use in the ElasticsearchPipeline. This transformer requires the instantiation
 * of an ElasticsearchObject, which supplies the necessary indexing information to the ElasticsearchEmitter.
 */
public abstract class ElasticsearchTransformer<T> implements ITransformerBase<T, ElasticsearchObject> {

    /**
     * Creates an ElasticsearchObject with the information contained in the record.
     */
    @Override
    public abstract ElasticsearchObject fromClass(T record) throws IOException;
}
