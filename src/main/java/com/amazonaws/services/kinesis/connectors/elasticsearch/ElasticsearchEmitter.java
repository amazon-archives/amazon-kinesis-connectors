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
package com.amazonaws.services.kinesis.connectors.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;

public class ElasticsearchEmitter implements IEmitter<ElasticsearchObject> {
    private static final Log LOG = LogFactory.getLog(ElasticsearchEmitter.class);

    /**
     * The settings key for the cluster name.
     * 
     * Defaults to elasticsearch.
     */
    private static final String ELASTICSEARCH_CLUSTER_NAME_KEY = "cluster.name";

    /**
     * The settings key for transport client sniffing. If set to true, this instructs the TransportClient to
     * find all nodes in the cluster, providing robustness if the original node were to become unavailable.
     * 
     * Defaults to false.
     */
    private static final String ELASTICSEARCH_CLIENT_TRANSPORT_SNIFF_KEY = "client.transport.sniff";

    /**
     * The settings key for ignoring the cluster name. Set to true to ignore cluster name validation
     * of connected nodes.
     * 
     * Defaults to false.
     */
    private static final String ELASTICSEARCH_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME_KEY =
            "client.transport.ignore_cluster_name";

    /**
     * The settings key for ping timeout. The time to wait for a ping response from a node.
     * 
     * Default to 5s.
     */
    private static final String ELASTICSEARCH_CLIENT_TRANSPORT_PING_TIMEOUT_KEY = "client.transport.ping_timeout";

    /**
     * The settings key for node sampler interval. How often to sample / ping the nodes listed and connected.
     * 
     * Defaults to 5s
     */
    private static final String ELASTICSEARCH_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL_KEY =
            "client.transport.nodes_sampler_interval";

    /**
     * The Elasticsearch client.
     */
    private final TransportClient elasticsearchClient;

    /**
     * The Elasticsearch endpoint.
     */
    private final String elasticsearchEndpoint;

    /**
     * The Elasticsearch port.
     */
    private final int elasticsearchPort;

    /**
     * The amount of time to wait in between unsuccessful index requests (in milliseconds).
     * 10 seconds = 10 * 1000 = 10000
     */
    private long BACKOFF_PERIOD = 10000;

    public ElasticsearchEmitter(KinesisConnectorConfiguration configuration) {
        Settings settings =
                ImmutableSettings.settingsBuilder()
                        .put(ELASTICSEARCH_CLUSTER_NAME_KEY, configuration.ELASTICSEARCH_CLUSTER_NAME)
                        .put(ELASTICSEARCH_CLIENT_TRANSPORT_SNIFF_KEY, configuration.ELASTICSEARCH_TRANSPORT_SNIFF)
                        .put(ELASTICSEARCH_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME_KEY,
                                configuration.ELASTICSEARCH_IGNORE_CLUSTER_NAME)
                        .put(ELASTICSEARCH_CLIENT_TRANSPORT_PING_TIMEOUT_KEY, configuration.ELASTICSEARCH_PING_TIMEOUT)
                        .put(ELASTICSEARCH_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL_KEY,
                                configuration.ELASTICSEARCH_NODE_SAMPLER_INTERVAL)
                        .build();
        elasticsearchEndpoint = configuration.ELASTICSEARCH_ENDPOINT;
        elasticsearchPort = configuration.ELASTICSEARCH_PORT;
        LOG.info("ElasticsearchEmitter using elasticsearch endpoint " + elasticsearchEndpoint + ":" + elasticsearchPort);
        elasticsearchClient = new TransportClient(settings);
        elasticsearchClient.addTransportAddress(new InetSocketTransportAddress(elasticsearchEndpoint, elasticsearchPort));
    }

    /**
     * Emits records to elasticsearch.
     * 1. Adds each record to a bulk index request, conditionally adding version, ttl or create if they were set in the
     * transformer.
     * 2. Executes the bulk request, returning any record specific failures to be retried by the connector library
     * pipeline, unless
     * outlined below.
     * 
     * Record specific failures (noted in the failure.getMessage() string)
     * - DocumentAlreadyExistsException means the record has create set to true, but a document already existed at the
     * specific index/type/id.
     * - VersionConflictEngineException means the record has a specific version number that did not match what existed
     * in elasticsearch.
     * To guarantee in order processing by the connector, when putting data use the same partition key for objects going
     * to the same
     * index/type/id and set sequence number for ordering.
     * - In either case, the emitter will assume that the record would fail again in the future and thus will not return
     * the record to
     * be retried.
     * 
     * Bulk request failures
     * - NoNodeAvailableException means the TransportClient could not connect to the cluster.
     * - A general Exception catches any other unexpected behavior.
     * - In either case the emitter will continue making attempts until the issue has been resolved. This is to ensure
     * that no data
     * loss occurs and simplifies restarting the application once issues have been fixed.
     */
    @Override
    public List<ElasticsearchObject> emit(UnmodifiableBuffer<ElasticsearchObject> buffer) throws IOException {
        List<ElasticsearchObject> records = buffer.getRecords();
        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        BulkRequestBuilder bulkRequest = elasticsearchClient.prepareBulk();
        for (ElasticsearchObject record : records) {
            IndexRequestBuilder indexRequestBuilder =
                    elasticsearchClient.prepareIndex(record.getIndex(), record.getType(), record.getId());
            indexRequestBuilder.setSource(record.getSource());
            Long version = record.getVersion();
            if (version != null) {
                indexRequestBuilder.setVersion(version);
            }
            Long ttl = record.getTtl();
            if (ttl != null) {
                indexRequestBuilder.setTTL(ttl);
            }
            Boolean create = record.getCreate();
            if (create != null) {
                indexRequestBuilder.setCreate(create);
            }
            bulkRequest.add(indexRequestBuilder);
        }

        while (true) {
            try {
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();

                BulkItemResponse[] responses = bulkResponse.getItems();
                List<ElasticsearchObject> failures = new ArrayList<ElasticsearchObject>();
                int numberOfSkippedRecords = 0;
                for (int i = 0; i < responses.length; i++) {
                    if (responses[i].isFailed()) {
                        LOG.error("Record failed with message: " + responses[i].getFailureMessage());
                        Failure failure = responses[i].getFailure();
                        if (failure.getMessage().contains("DocumentAlreadyExistsException")
                                || failure.getMessage().contains("VersionConflictEngineException")) {
                            numberOfSkippedRecords++;
                        } else {
                            failures.add(records.get(i));
                        }
                    }
                }
                LOG.info("Emitted " + (records.size() - failures.size() - numberOfSkippedRecords)
                        + " records to Elasticsearch");
                if (!failures.isEmpty()) {
                    printClusterStatus();
                    LOG.warn("Returning " + failures.size() + " records as failed");
                }
                return failures;
            } catch (NoNodeAvailableException nnae) {
                LOG.error("No nodes found at " + elasticsearchEndpoint + ":" + elasticsearchPort + ". Retrying in "
                        + BACKOFF_PERIOD + " milliseconds", nnae);
                sleep(BACKOFF_PERIOD);
            } catch (Exception e) {
                LOG.error("ElasticsearchEmitter threw an unexpected exception ", e);
                sleep(BACKOFF_PERIOD);
            }
        }

    }

    @Override
    public void fail(List<ElasticsearchObject> records) {
        for (ElasticsearchObject record : records) {
            LOG.error("Record failed: " + record);
        }
    }

    @Override
    public void shutdown() {
        elasticsearchClient.close();
    }

    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
        }
    }

    private void printClusterStatus() {
        ClusterHealthRequestBuilder healthRequestBuilder = elasticsearchClient.admin().cluster().prepareHealth();
        ClusterHealthResponse response = healthRequestBuilder.execute().actionGet();
        if (response.getStatus().equals(ClusterHealthStatus.RED)) {
            LOG.error("Cluster health is RED. Indexing ability will be limited");
        } else if (response.getStatus().equals(ClusterHealthStatus.YELLOW)) {
            LOG.warn("Cluster health is YELLOW.");
        } else if (response.getStatus().equals(ClusterHealthStatus.GREEN)) {
            LOG.info("Cluster health is GREEN.");
        }
    }
}
