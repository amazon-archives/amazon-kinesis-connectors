/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
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
