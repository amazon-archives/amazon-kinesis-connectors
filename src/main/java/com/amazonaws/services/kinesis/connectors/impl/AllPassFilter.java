/*
 * Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
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
