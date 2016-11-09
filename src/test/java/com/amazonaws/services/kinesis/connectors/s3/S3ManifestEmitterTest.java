/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
