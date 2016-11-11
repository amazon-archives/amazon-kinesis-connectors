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
