/*
 * Copyright 2013-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.rmi.dgc.VMID;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

/**
 * This class contains constants used to configure AWS Services in Amazon Kinesis Connectors. The user
 * should use System properties to set their proper configuration. An instance of
 * KinesisConnectorConfiguration is created with System properties and an AWSCredentialsProvider.
 * For example:
 * 
 * <pre>
 * Properties prop = new Properties();
 * prop.put(KinesisConnectorConfiguration.PROP_APP_NAME, &quot;MyKinesisConnector&quot;);
 * KinesisConnectorConfiguration config =
 *         new KinesisConnectorConfiguration(prop, new DefaultAWSCredentialsProviderChain());
 * </pre>
 * 
 */
public class KinesisConnectorConfiguration {
    private static final Log LOG = LogFactory.getLog(KinesisConnectorConfiguration.class);
    public static final String KINESIS_CONNECTOR_USER_AGENT = "amazon-kinesis-connector-java-1.3.0";

    // Connector App Property Keys
    public static final String PROP_APP_NAME = "appName";
    public static final String PROP_CONNECTOR_DESTINATION = "connectorDestination";
    public static final String PROP_RETRY_LIMIT = "retryLimit";
    public static final String PROP_BACKOFF_INTERVAL = "backoffInterval";
    public static final String PROP_KINESIS_ENDPOINT = "kinesisEndpoint";
    public static final String PROP_KINESIS_INPUT_STREAM = "kinesisInputStream";
    public static final String PROP_KINESIS_INPUT_STREAM_SHARD_COUNT = "kinesisInputStreamShardCount";
    public static final String PROP_KINESIS_OUTPUT_STREAM = "kinesisOutputStream";
    public static final String PROP_KINESIS_OUTPUT_STREAM_SHARD_COUNT = "kinesisOutputStreamShardCount";
    public static final String PROP_WORKER_ID = "workerID";
    public static final String PROP_FAILOVER_TIME = "failoverTime";
    public static final String PROP_MAX_RECORDS = "maxRecords";
    public static final String PROP_INITIAL_POSITION_IN_STREAM = "initialPositionInStream";
    public static final String PROP_IDLE_TIME_BETWEEN_READS = "idleTimeBetweenReads";
    public static final String PROP_PARENT_SHARD_POLL_INTERVAL = "parentShardPollInterval";
    public static final String PROP_SHARD_SYNC_INTERVAL = "shardSyncInterval";
    public static final String PROP_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST = "callProcessRecordsEvenForEmptyList";
    public static final String PROP_CLEANUP_TERMINATED_SHARDS_BEFORE_EXPIRY = "cleanupTerminatedShardsBeforeExpiry";
    public static final String PROP_REGION_NAME = "regionName";
    public static final String PROP_BATCH_RECORDS_IN_PUT_REQUEST = "batchRecordsInPutRequest";
    public static final String PROP_S3_ENDPOINT = "s3Endpoint";
    public static final String PROP_S3_BUCKET = "s3Bucket";
    public static final String PROP_REDSHIFT_ENDPOINT = "redshiftEndpoint";
    public static final String PROP_REDSHIFT_USERNAME = "redshiftUsername";
    public static final String PROP_REDSHIFT_PASSWORD = "redshiftPassword";
    public static final String PROP_REDSHIFT_URL = "redshiftURL";
    public static final String PROP_REDSHIFT_DATA_TABLE = "redshiftDataTable";
    public static final String PROP_REDSHIFT_FILE_TABLE = "redshiftFileTable";
    public static final String PROP_REDSHIFT_FILE_KEY_COLUMN = "redshiftFileKeyColumn";
    public static final String PROP_REDSHIFT_DATA_DELIMITER = "redshiftDataDelimiter";
    public static final String PROP_REDSHIFT_COPY_MANDATORY = "redshiftCopyMandatory";
    public static final String PROP_BUFFER_RECORD_COUNT_LIMIT = "bufferRecordCountLimit";
    public static final String PROP_BUFFER_BYTE_SIZE_LIMIT = "bufferByteSizeLimit";
    public static final String PROP_BUFFER_MILLISECONDS_LIMIT = "bufferMillisecondsLimit";
    public static final String PROP_DYNAMODB_ENDPOINT = "dynamoDBEndpoint";
    public static final String PROP_DYNAMODB_DATA_TABLE_NAME = "dynamoDBDataTableName";
    public static final String PROP_CLOUDWATCH_NAMESPACE = "cloudWatchNamespace";
    public static final String PROP_CLOUDWATCH_BUFFER_TIME = "cloudWatchBufferTime";
    public static final String PROP_CLOUDWATCH_MAX_QUEUE_SIZE = "cloudWatchMaxQueueSize";
    public static final String PROP_ELASTICSEARCH_CLUSTER_NAME = "elasticsearchClusterName";
    public static final String PROP_ELASTICSEARCH_ENDPOINT = "elasticsearchEndpoint";
    public static final String PROP_ELASTICSEARCH_PORT = "elasticsearchPort";
    public static final String PROP_ELASTICSEARCH_TRANSPORT_SNIFF = "clientTransportSniff";
    public static final String PROP_ELASTICSEARCH_IGNORE_CLUSTER_NAME = "clientTransportIgnoreClusterName";
    public static final String PROP_ELASTICSEARCH_PING_TIMEOUT = "clientTransportPingTimeout";
    public static final String PROP_ELASTICSEARCH_NODE_SAMPLER_INTERVAL = "clientTransportNodesSamplerInterval";
    public static final String PROP_ELASTICSEARCH_DEFAULT_INDEX_NAME = "elasticsearchDefaultIndexName";
    public static final String PROP_ELASTICSEARCH_DEFAULT_TYPE_NAME = "elasticsearchDefaultTypeName";
    public static final String PROP_ELASTICSEARCH_CLOUDFORMATION_TEMPLATE_URL =
            "elasticsearchCloudFormationTemplateUrl";
    public static final String PROP_ELASTICSEARCH_CLOUDFORMATION_STACK_NAME = "elasticsearchCloudFormationStackName";
    public static final String PROP_ELASTICSEARCH_VERSION_NUMBER = "elasticsearchVersionNumber";
    public static final String PROP_ELASTICSEARCH_CLOUDFORMATION_KEY_PAIR_NAME =
            "elasticsearchCloudFormationKeyPairName";
    public static final String PROP_ELASTICSEARCH_CLOUDFORMATION_CLUSTER_INSTANCE_TYPE =
            "elasticsearchCloudFormationClusterInstanceType";
    public static final String PROP_ELASTICSEARCH_CLOUDFORMATION_SSH_LOCATION =
            "elasticsearchCloudFormationSSHLocation";
    public static final String PROP_ELASTICSEARCH_CLOUDFORMATION_CLUSTER_SIZE =
            "elasticsearchCloudFormationClusterSize";

    // Default Connector App Constants
    public static final String DEFAULT_APP_NAME = "KinesisConnector";
    public static final String DEFAULT_CONNECTOR_DESTINATION = "generic";
    public static final int DEFAULT_RETRY_LIMIT = 3;
    public static final long DEFAULT_BACKOFF_INTERVAL = 1000L * 10;
    public static final long DEFAULT_BUFFER_RECORD_COUNT_LIMIT = 1000L;
    public static final long DEFAULT_BUFFER_BYTE_SIZE_LIMIT = 1024 * 1024L;
    public static final long DEFAULT_BUFFER_MILLISECONDS_LIMIT = Long.MAX_VALUE;
    public static final boolean DEFAULT_BATCH_RECORDS_IN_PUT_REQUEST = false;

    // Default Amazon Kinesis Constants
    public static final String DEFAULT_KINESIS_ENDPOINT = null;
    public static final String DEFAULT_KINESIS_INPUT_STREAM = "kinesisInputStream";
    public static final String DEFAULT_KINESIS_OUTPUT_STREAM = "kinesisOutputStream";
    public static final int DEFAULT_KINESIS_STREAM_SHARD_COUNT = 1;

    // Default Amazon Kinesis Client Library Constants
    public static final String DEFAULT_WORKER_ID = new VMID().toString();
    public static final long DEFAULT_FAILOVER_TIME = 30000L;
    public static final int DEFAULT_MAX_RECORDS = 10000;
    public static final InitialPositionInStream DEFAULT_INITIAL_POSITION_IN_STREAM =
            InitialPositionInStream.TRIM_HORIZON;
    public static final long DEFAULT_IDLE_TIME_BETWEEN_READS = 1000L;
    public static final long DEFAULT_PARENT_SHARD_POLL_INTERVAL = 10000L;
    public static final long DEFAULT_SHARD_SYNC_INTERVAL = 60000L;
    // CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST must be set to true for bufferMillisecondsLimit to work
    public static final boolean DEFAULT_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST = true;
    public static final boolean DEFAULT_CLEANUP_TERMINATED_SHARDS_BEFORE_EXPIRY = false;
    public static final String DEFAULT_REGION_NAME = "us-east-1";

    // Default Amazon S3 Constants
    public static final String DEFAULT_S3_ENDPOINT = "https://s3.amazonaws.com";
    public static final String DEFAULT_S3_BUCKET = "kinesis-bucket";

    // Default Amazon Redshift Constants
    public static final String DEFAULT_REDSHIFT_ENDPOINT = "https://redshift.us-east-1.amazonaws.com";
    public static final String DEFAULT_REDSHIFT_USERNAME = null;
    public static final String DEFAULT_REDSHIFT_PASSWORD = null;
    public static final String DEFAULT_REDSHIFT_URL = null;
    public static final String DEFAULT_REDSHIFT_DATA_TABLE = "users";
    public static final String DEFAULT_REDSHIFT_FILE_TABLE = "files";
    public static final String DEFAULT_REDSHIFT_FILE_KEY_COLUMN = "file";
    public static final Character DEFAULT_REDSHIFT_DATA_DELIMITER = '|';
    public static final boolean DEFAULT_REDSHIFT_COPY_MANDATORY = true;

    // Default Amazon DynamoDB Constants
    public static final String DEFAULT_DYNAMODB_ENDPOINT = "dynamodb.us-east-1.amazonaws.com";
    public static final String DEFAULT_DYNAMODB_DATA_TABLE_NAME = "dynamodb_emitter_test";

    // Default Amazon CloudWatch Constants
    public static final String DEFAULT_CLOUDWATCH_NAMESPACE = DEFAULT_APP_NAME;
    public static final long DEFAULT_CLOUDWATCH_BUFFER_TIME = 10 * 1000L;
    public static final int DEFAULT_CLOUDWATCH_MAX_QUEUE_SIZE = 10000;

    // Default Amazon Elasticsearch Constraints
    public static final String DEFAULT_ELASTICSEARCH_CLUSTER_NAME = "elasticsearch";
    public static final String DEFAULT_ELASTICSEARCH_ENDPOINT = "localhost";
    public static final int DEFAULT_ELASTICSEARCH_PORT = 9300;
    public static final boolean DEFAULT_ELASTICSEARCH_TRANSPORT_SNIFF = false;
    public static final boolean DEFAULT_ELASTICSEARCH_IGNORE_CLUSTER_NAME = false;
    public static final String DEFAULT_ELASTICSEARCH_PING_TIMEOUT = "5s";
    public static final String DEFAULT_ELASTICSEARCH_NODE_SAMPLER_INTERVAL = "5s";
    public static final String DEFAULT_ELASTICSEARCH_DEFAULT_INDEX_NAME = "index";
    public static final String DEFAULT_ELASTICSEARCH_DEFAULT_TYPE_NAME = "type";
    public static final String DEFAULT_ELASTICSEARCH_CLOUDFORMATION_TEMPLATE_URL = "Elasticsearch.template";
    public static final String DEFAULT_ELASTICSEARCH_CLOUDFORMATION_STACK_NAME = "kinesisElasticsearchSample";
    public static final String DEFAULT_ELASTICSEARCH_VERSION_NUMBER = "1.2.1";
    public static final String DEFAULT_ELASTICSEARCH_CLOUDFORMATION_KEY_PAIR_NAME = "";
    public static final String DEFAULT_ELASTICSEARCH_CLOUDFORMATION_CLUSTER_INSTANCE_TYPE = "m1.small";
    public static final String DEFAULT_ELASTICSEARCH_CLOUDFORMATION_SSH_LOCATION = "0.0.0.0/0";
    public static final String DEFAULT_ELASTICSEARCH_CLOUDFORMATION_CLUSTER_SIZE = "3";

    private static final String CONNECTION_DESTINATION_PREFIX = "amazon-kinesis-connector-to-";

    // Configurable program variables
    public final AWSCredentialsProvider AWS_CREDENTIALS_PROVIDER;
    public final String APP_NAME;
    public final String CONNECTOR_DESTINATION;
    public final long BACKOFF_INTERVAL;
    public final int RETRY_LIMIT;
    public final long BUFFER_RECORD_COUNT_LIMIT;
    public final long BUFFER_BYTE_SIZE_LIMIT;
    public final long BUFFER_MILLISECONDS_LIMIT;
    public final boolean BATCH_RECORDS_IN_PUT_REQUEST;

    public final String KINESIS_ENDPOINT;
    public final String KINESIS_INPUT_STREAM;
    public final int KINESIS_INPUT_STREAM_SHARD_COUNT;
    public final String KINESIS_OUTPUT_STREAM;
    public final int KINESIS_OUTPUT_STREAM_SHARD_COUNT;

    public final String WORKER_ID;
    public final long FAILOVER_TIME;
    public final int MAX_RECORDS;
    public final InitialPositionInStream INITIAL_POSITION_IN_STREAM;
    public final long IDLE_TIME_BETWEEN_READS;
    public final long PARENT_SHARD_POLL_INTERVAL;
    public final long SHARD_SYNC_INTERVAL;
    public final boolean CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST;
    public final boolean CLEANUP_TERMINATED_SHARDS_BEFORE_EXPIRY;
    public final String REGION_NAME;
    public final String S3_ENDPOINT;
    public final String S3_BUCKET;
    public final String REDSHIFT_ENDPOINT;
    public final String REDSHIFT_USERNAME;
    public final String REDSHIFT_PASSWORD;
    public String REDSHIFT_URL;
    public final String REDSHIFT_DATA_TABLE;
    public final String REDSHIFT_FILE_TABLE;
    public final String REDSHIFT_FILE_KEY_COLUMN;
    public final Character REDSHIFT_DATA_DELIMITER;
    public final boolean REDSHIFT_COPY_MANDATORY;
    public final String DYNAMODB_ENDPOINT;
    public final String DYNAMODB_DATA_TABLE_NAME;
    public final String CLOUDWATCH_NAMESPACE;
    public final long CLOUDWATCH_BUFFER_TIME;
    public final int CLOUDWATCH_MAX_QUEUE_SIZE;
    public final String ELASTICSEARCH_CLUSTER_NAME;
    public String ELASTICSEARCH_ENDPOINT;
    public final int ELASTICSEARCH_PORT;
    public final boolean ELASTICSEARCH_TRANSPORT_SNIFF;
    public final boolean ELASTICSEARCH_IGNORE_CLUSTER_NAME;
    public final String ELASTICSEARCH_PING_TIMEOUT;
    public final String ELASTICSEARCH_NODE_SAMPLER_INTERVAL;
    public final String ELASTICSEARCH_DEFAULT_INDEX_NAME;
    public final String ELASTICSEARCH_DEFAULT_TYPE_NAME;
    public final String ELASTICSEARCH_CLOUDFORMATION_TEMPLATE_URL;
    public final String ELASTICSEARCH_CLOUDFORMATION_STACK_NAME;
    public final String ELASTICSEARCH_VERSION_NUMBER;
    public final String ELASTICSEARCH_CLOUDFORMATION_KEY_PAIR_NAME;
    public final String ELASTICSEARCH_CLOUDFORMATION_CLUSTER_INSTANCE_TYPE;
    public final String ELASTICSEARCH_CLOUDFORMATION_SSH_LOCATION;
    public final String ELASTICSEARCH_CLOUDFORMATION_CLUSTER_SIZE;

    /**
     * Configure the connector application with any set of properties that are unique to the application. Any
     * unspecified property will be set to a default value.
     * 
     * @param properties
     *        the System properties that will be used to configure KinesisConnectors
     */
    public KinesisConnectorConfiguration(Properties properties, AWSCredentialsProvider credentialsProvider) {
        AWS_CREDENTIALS_PROVIDER = credentialsProvider;

        // Connector configuration
        APP_NAME = properties.getProperty(PROP_APP_NAME, DEFAULT_APP_NAME);
        CONNECTOR_DESTINATION =
                CONNECTION_DESTINATION_PREFIX
                        + properties.getProperty(PROP_CONNECTOR_DESTINATION, DEFAULT_CONNECTOR_DESTINATION);
        RETRY_LIMIT = getIntegerProperty(PROP_RETRY_LIMIT, DEFAULT_RETRY_LIMIT, properties);
        BACKOFF_INTERVAL = getLongProperty(PROP_BACKOFF_INTERVAL, DEFAULT_BACKOFF_INTERVAL, properties);
        BUFFER_RECORD_COUNT_LIMIT =
                getLongProperty(PROP_BUFFER_RECORD_COUNT_LIMIT, DEFAULT_BUFFER_RECORD_COUNT_LIMIT, properties);
        BUFFER_BYTE_SIZE_LIMIT =
                getLongProperty(PROP_BUFFER_BYTE_SIZE_LIMIT, DEFAULT_BUFFER_BYTE_SIZE_LIMIT, properties);
        BUFFER_MILLISECONDS_LIMIT =
                getLongProperty(PROP_BUFFER_MILLISECONDS_LIMIT, DEFAULT_BUFFER_MILLISECONDS_LIMIT, properties);
        BATCH_RECORDS_IN_PUT_REQUEST =
                getBooleanProperty(PROP_BATCH_RECORDS_IN_PUT_REQUEST, DEFAULT_BATCH_RECORDS_IN_PUT_REQUEST, properties);

        // Amazon Kinesis configuration
        KINESIS_ENDPOINT = properties.getProperty(PROP_KINESIS_ENDPOINT, DEFAULT_KINESIS_ENDPOINT);
        KINESIS_INPUT_STREAM = properties.getProperty(PROP_KINESIS_INPUT_STREAM, DEFAULT_KINESIS_INPUT_STREAM);
        KINESIS_INPUT_STREAM_SHARD_COUNT =
                getIntegerProperty(PROP_KINESIS_INPUT_STREAM_SHARD_COUNT,
                        DEFAULT_KINESIS_STREAM_SHARD_COUNT,
                        properties);
        KINESIS_OUTPUT_STREAM = properties.getProperty(PROP_KINESIS_OUTPUT_STREAM, DEFAULT_KINESIS_OUTPUT_STREAM);
        KINESIS_OUTPUT_STREAM_SHARD_COUNT =
                getIntegerProperty(PROP_KINESIS_OUTPUT_STREAM_SHARD_COUNT,
                        DEFAULT_KINESIS_STREAM_SHARD_COUNT,
                        properties);

        // Amazon S3 configuration
        S3_ENDPOINT = properties.getProperty(PROP_S3_ENDPOINT, DEFAULT_S3_ENDPOINT);
        S3_BUCKET = properties.getProperty(PROP_S3_BUCKET, DEFAULT_S3_BUCKET);

        // Amazon Redshift configuration
        REDSHIFT_ENDPOINT = properties.getProperty(PROP_REDSHIFT_ENDPOINT, DEFAULT_REDSHIFT_ENDPOINT);
        REDSHIFT_USERNAME = properties.getProperty(PROP_REDSHIFT_USERNAME, DEFAULT_REDSHIFT_USERNAME);
        REDSHIFT_PASSWORD = properties.getProperty(PROP_REDSHIFT_PASSWORD, DEFAULT_REDSHIFT_PASSWORD);
        REDSHIFT_URL = properties.getProperty(PROP_REDSHIFT_URL, DEFAULT_REDSHIFT_URL);
        REDSHIFT_DATA_TABLE = properties.getProperty(PROP_REDSHIFT_DATA_TABLE, DEFAULT_REDSHIFT_DATA_TABLE);
        REDSHIFT_FILE_TABLE = properties.getProperty(PROP_REDSHIFT_FILE_TABLE, DEFAULT_REDSHIFT_FILE_TABLE);
        REDSHIFT_FILE_KEY_COLUMN =
                properties.getProperty(PROP_REDSHIFT_FILE_KEY_COLUMN, DEFAULT_REDSHIFT_FILE_KEY_COLUMN);
        REDSHIFT_DATA_DELIMITER =
                getCharacterProperty(PROP_REDSHIFT_DATA_DELIMITER, DEFAULT_REDSHIFT_DATA_DELIMITER, properties);
        REDSHIFT_COPY_MANDATORY =
                getBooleanProperty(PROP_REDSHIFT_COPY_MANDATORY, DEFAULT_REDSHIFT_COPY_MANDATORY, properties);

        // Amazon DynamoDB configuration
        DYNAMODB_ENDPOINT = properties.getProperty(PROP_DYNAMODB_ENDPOINT, DEFAULT_DYNAMODB_ENDPOINT);
        DYNAMODB_DATA_TABLE_NAME =
                properties.getProperty(PROP_DYNAMODB_DATA_TABLE_NAME, DEFAULT_DYNAMODB_DATA_TABLE_NAME);

        // Amazon CloudWatch configuration
        CLOUDWATCH_NAMESPACE = properties.getProperty(PROP_CLOUDWATCH_NAMESPACE, DEFAULT_CLOUDWATCH_NAMESPACE);
        CLOUDWATCH_BUFFER_TIME =
                getLongProperty(PROP_CLOUDWATCH_BUFFER_TIME, DEFAULT_CLOUDWATCH_BUFFER_TIME, properties);
        CLOUDWATCH_MAX_QUEUE_SIZE =
                getIntegerProperty(PROP_CLOUDWATCH_MAX_QUEUE_SIZE, DEFAULT_CLOUDWATCH_MAX_QUEUE_SIZE, properties);

        // Elasticsearch configuration
        ELASTICSEARCH_CLUSTER_NAME =
                properties.getProperty(PROP_ELASTICSEARCH_CLUSTER_NAME, DEFAULT_ELASTICSEARCH_CLUSTER_NAME);
        ELASTICSEARCH_ENDPOINT = properties.getProperty(PROP_ELASTICSEARCH_ENDPOINT, DEFAULT_ELASTICSEARCH_ENDPOINT);
        ELASTICSEARCH_PORT = getIntegerProperty(PROP_ELASTICSEARCH_PORT, DEFAULT_ELASTICSEARCH_PORT, properties);
        ELASTICSEARCH_TRANSPORT_SNIFF =
                getBooleanProperty(PROP_ELASTICSEARCH_TRANSPORT_SNIFF,
                        DEFAULT_ELASTICSEARCH_TRANSPORT_SNIFF,
                        properties);

        ELASTICSEARCH_IGNORE_CLUSTER_NAME =
                getBooleanProperty(PROP_ELASTICSEARCH_IGNORE_CLUSTER_NAME,
                        DEFAULT_ELASTICSEARCH_IGNORE_CLUSTER_NAME,
                        properties);
        ELASTICSEARCH_PING_TIMEOUT =
                properties.getProperty(PROP_ELASTICSEARCH_PING_TIMEOUT, DEFAULT_ELASTICSEARCH_PING_TIMEOUT);
        ELASTICSEARCH_NODE_SAMPLER_INTERVAL =
                properties.getProperty(PROP_ELASTICSEARCH_NODE_SAMPLER_INTERVAL,
                        DEFAULT_ELASTICSEARCH_NODE_SAMPLER_INTERVAL);
        ELASTICSEARCH_DEFAULT_INDEX_NAME =
                properties.getProperty(PROP_ELASTICSEARCH_DEFAULT_INDEX_NAME, DEFAULT_ELASTICSEARCH_DEFAULT_INDEX_NAME);
        ELASTICSEARCH_DEFAULT_TYPE_NAME =
                properties.getProperty(PROP_ELASTICSEARCH_DEFAULT_TYPE_NAME, DEFAULT_ELASTICSEARCH_DEFAULT_TYPE_NAME);
        ELASTICSEARCH_CLOUDFORMATION_TEMPLATE_URL =
                properties.getProperty(PROP_ELASTICSEARCH_CLOUDFORMATION_TEMPLATE_URL,
                        DEFAULT_ELASTICSEARCH_CLOUDFORMATION_TEMPLATE_URL);
        ELASTICSEARCH_CLOUDFORMATION_STACK_NAME =
                properties.getProperty(PROP_ELASTICSEARCH_CLOUDFORMATION_STACK_NAME,
                        DEFAULT_ELASTICSEARCH_CLOUDFORMATION_STACK_NAME);
        ELASTICSEARCH_VERSION_NUMBER =
                properties.getProperty(PROP_ELASTICSEARCH_VERSION_NUMBER, DEFAULT_ELASTICSEARCH_VERSION_NUMBER);
        ELASTICSEARCH_CLOUDFORMATION_KEY_PAIR_NAME =
                properties.getProperty(PROP_ELASTICSEARCH_CLOUDFORMATION_KEY_PAIR_NAME,
                        DEFAULT_ELASTICSEARCH_CLOUDFORMATION_KEY_PAIR_NAME);
        ELASTICSEARCH_CLOUDFORMATION_CLUSTER_INSTANCE_TYPE =
                properties.getProperty(PROP_ELASTICSEARCH_CLOUDFORMATION_CLUSTER_INSTANCE_TYPE,
                        DEFAULT_ELASTICSEARCH_CLOUDFORMATION_CLUSTER_INSTANCE_TYPE);
        ELASTICSEARCH_CLOUDFORMATION_SSH_LOCATION =
                properties.getProperty(PROP_ELASTICSEARCH_CLOUDFORMATION_SSH_LOCATION,
                        DEFAULT_ELASTICSEARCH_CLOUDFORMATION_SSH_LOCATION);
        ELASTICSEARCH_CLOUDFORMATION_CLUSTER_SIZE =
                properties.getProperty(PROP_ELASTICSEARCH_CLOUDFORMATION_CLUSTER_SIZE,
                        DEFAULT_ELASTICSEARCH_CLOUDFORMATION_CLUSTER_SIZE);

        // Amazon Kinesis Client Library configuration
        WORKER_ID = properties.getProperty(PROP_WORKER_ID, DEFAULT_WORKER_ID);
        FAILOVER_TIME = getLongProperty(PROP_FAILOVER_TIME, DEFAULT_FAILOVER_TIME, properties);
        MAX_RECORDS = getIntegerProperty(PROP_MAX_RECORDS, DEFAULT_MAX_RECORDS, properties);
        INITIAL_POSITION_IN_STREAM =
                getInitialPositionInStreamProperty(PROP_INITIAL_POSITION_IN_STREAM,
                        DEFAULT_INITIAL_POSITION_IN_STREAM,
                        properties);
        IDLE_TIME_BETWEEN_READS =
                getLongProperty(PROP_IDLE_TIME_BETWEEN_READS, DEFAULT_IDLE_TIME_BETWEEN_READS, properties);
        PARENT_SHARD_POLL_INTERVAL =
                getLongProperty(PROP_PARENT_SHARD_POLL_INTERVAL, DEFAULT_PARENT_SHARD_POLL_INTERVAL, properties);
        SHARD_SYNC_INTERVAL = getLongProperty(PROP_SHARD_SYNC_INTERVAL, DEFAULT_SHARD_SYNC_INTERVAL, properties);
        CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST =
                getBooleanProperty(PROP_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST,
                        DEFAULT_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST,
                        properties);
        CLEANUP_TERMINATED_SHARDS_BEFORE_EXPIRY =
                getBooleanProperty(PROP_CLEANUP_TERMINATED_SHARDS_BEFORE_EXPIRY,
                        DEFAULT_CLEANUP_TERMINATED_SHARDS_BEFORE_EXPIRY,
                        properties);
        REGION_NAME = properties.getProperty(PROP_REGION_NAME, DEFAULT_REGION_NAME);
    }

    private boolean getBooleanProperty(String property, boolean defaultValue, Properties properties) {
        String propertyValue = properties.getProperty(property, Boolean.toString(defaultValue));
        return Boolean.parseBoolean(propertyValue);
    }

    private long getLongProperty(String property, long defaultValue, Properties properties) {
        String propertyValue = properties.getProperty(property, Long.toString(defaultValue));
        try {
            return Long.parseLong(propertyValue.trim());
        } catch (NumberFormatException e) {
            LOG.error(e);
            return defaultValue;
        }
    }

    private int getIntegerProperty(String property, int defaultValue, Properties properties) {
        String propertyValue = properties.getProperty(property, Integer.toString(defaultValue));
        try {
            return Integer.parseInt(propertyValue.trim());
        } catch (NumberFormatException e) {
            LOG.error(e);
            return defaultValue;
        }
    }

    private char getCharacterProperty(String property, char defaultValue, Properties properties) {
        String propertyValue = properties.getProperty(property, Character.toString(defaultValue));
        if (propertyValue.length() == 1) {
            return propertyValue.charAt(0);
        }
        return defaultValue;
    }

    private InitialPositionInStream getInitialPositionInStreamProperty(String property,
            InitialPositionInStream defaultInitialPositionInInputStream,
            Properties properties) {
        String propertyValue = properties.getProperty(property, defaultInitialPositionInInputStream.toString());
        try {
            return InitialPositionInStream.valueOf(propertyValue);
        } catch (Exception e) {
            LOG.error(e);
            return defaultInitialPositionInInputStream;
        }
    }
}
