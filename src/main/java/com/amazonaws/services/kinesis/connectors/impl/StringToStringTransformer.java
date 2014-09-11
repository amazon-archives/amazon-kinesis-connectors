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
package com.amazonaws.services.kinesis.connectors.impl;

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.model.Record;

/**
 * This class is an implementation of the ITransformer to transform between raw Amazon Kinesis records and
 * strings. It assumes that the Record parameter of toClass() is a byte array representation of a
 * string. This is useful for the RedshiftManifestEmitter to perform an Amazon Redshift copy on a file name
 * specified in a String.
 * 
 */
public class StringToStringTransformer implements ITransformer<String, String> {

    @Override
    public String toClass(Record record) {
        return new String(record.getData().array());
    }

    @Override
    public String fromClass(String record) {
        return record;
    }
}
