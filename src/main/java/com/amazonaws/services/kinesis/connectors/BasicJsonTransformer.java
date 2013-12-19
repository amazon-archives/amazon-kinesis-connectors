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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class implements the ITransformer interface and provides an implementation of the toClass()
 * method for deserializing and serializing JSON strings. The constructor takes the class to
 * transform to/from JSON. The Record parameter of the toClass() method is expected to contain a
 * byte representation of a JSON string.
 * 
 * @param <T>
 */
public abstract class BasicJsonTransformer<T, U> implements ITransformer<T, U> {
    private static final Log LOG = LogFactory.getLog(BasicJsonTransformer.class);
    protected Class<T> inputClass;

    public BasicJsonTransformer(Class<T> inputClass) {
        this.inputClass = inputClass;
    }

    @Override
    public T toClass(Record record) throws IOException {
        try {
            return new ObjectMapper().readValue(record.getData().array(), this.inputClass);
        } catch (IOException e) {
            String message = "Error parsing record from JSON: " + new String(record.getData().array());
            LOG.error(message, e);
            throw new IOException(message, e);
        }
    }

}
