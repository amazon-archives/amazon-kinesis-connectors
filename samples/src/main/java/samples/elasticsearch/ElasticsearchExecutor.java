/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.elasticsearch;

import samples.KinesisConnectorExecutor;
import samples.KinesisMessageModel;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject;

public class ElasticsearchExecutor extends KinesisConnectorExecutor<KinesisMessageModel, ElasticsearchObject> {
    private static String configFile = "ElasticsearchSample.properties";

    /**
     * Creates a new ElasticsearchExecutor.
     * 
     * @param configFile The name of the configuration file to look for on the classpath.
     */
    public ElasticsearchExecutor(String configFile) {
        super(configFile);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<KinesisMessageModel, ElasticsearchObject>
            getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<KinesisMessageModel, ElasticsearchObject>(new ElasticsearchPipeline(),
                config);
    }

    /**
     * Main method starts and runs the ElasticsearchExecutor.
     * 
     * @param args
     */
    public static void main(String[] args) {
        try {
            Class.forName("org.elasticsearch.client.transport.TransportClient");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not load Elasticsearch jar", e);
        }
        try {
            Class.forName("org.apache.lucene.util.Version");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not load Apache Lucene jar", e);
        }

        KinesisConnectorExecutor<KinesisMessageModel, ElasticsearchObject> elasticsearchExecutor =
                new ElasticsearchExecutor(configFile);
        elasticsearchExecutor.run();
    }

}
