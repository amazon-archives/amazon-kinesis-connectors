/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples;

import com.amazonaws.services.kinesis.metrics.impl.CWMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

/**
 * This class defines the execution of an Amazon Kinesis Connector with Amazon CloudWatch metrics.
 * 
 */
public abstract class KinesisConnectorMetricsExecutor<T, U> extends KinesisConnectorExecutor<T, U> {

    /**
     * Creates a new KinesisConnectorMetricsExecutor.
     * 
     * @param configFile The name of the configuration file to look for on the classpath
     */
    public KinesisConnectorMetricsExecutor(String configFile) {
        super(configFile);

        // Amazon CloudWatch Metrics Factory used to emit metrics in KCL
        IMetricsFactory mFactory =
                new CWMetricsFactory(config.AWS_CREDENTIALS_PROVIDER,
                        config.CLOUDWATCH_NAMESPACE,
                        config.CLOUDWATCH_BUFFER_TIME,
                        config.CLOUDWATCH_MAX_QUEUE_SIZE);
        super.initialize(config, mFactory);
    }
}
