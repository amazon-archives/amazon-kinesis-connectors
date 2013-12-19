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
package com.amazonaws.services.kinesis.connectors.impl;

import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;

/**
 * This class is a basic implementation of IFilter that returns true for all records.
 * 
 * @param <T>
 */
public class AllPassFilter<T> implements IFilter<T> {

    @Override
    public boolean keepRecord(T record) {
        return true;
    }

}
