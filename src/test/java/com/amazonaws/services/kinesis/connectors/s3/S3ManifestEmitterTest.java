/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.connectors.s3;

import java.util.Properties;

import static org.easymock.EasyMock.createControl;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.s3.S3ManifestEmitter;

public class S3ManifestEmitterTest {

    IMocksControl control;
    AWSCredentialsProvider credentials;
    
    @Before
    public void setup() {
        control = createControl();
        credentials = control.createMock(AWSCredentialsProvider.class);
    }

    @Test
    public void testSupportsNullEndpoint() {
        // do not set KINESIS_ENDPOINT
        KinesisConnectorConfiguration config = new KinesisConnectorConfiguration(new Properties(), credentials);
        new S3ManifestEmitter(config);
    }

}
