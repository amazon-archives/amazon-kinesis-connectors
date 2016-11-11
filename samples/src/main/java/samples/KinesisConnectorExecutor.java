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
package samples;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import samples.utils.CloudFormationUtils;
import samples.utils.DynamoDBUtils;
import samples.utils.EC2Utils;
import samples.utils.KinesisUtils;
import samples.utils.RedshiftUtils;
import samples.utils.S3Utils;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * This class defines the execution of a Amazon Kinesis Connector.
 * 
 */
public abstract class KinesisConnectorExecutor<T, U> extends KinesisConnectorExecutorBase<T, U> {

    private static final Log LOG = LogFactory.getLog(KinesisConnectorExecutor.class);

    // Create AWS Resource constants
    private static final String CREATE_KINESIS_INPUT_STREAM = "createKinesisInputStream";
    private static final String CREATE_KINESIS_OUTPUT_STREAM = "createKinesisOutputStream";
    private static final String CREATE_DYNAMODB_DATA_TABLE = "createDynamoDBDataTable";
    private static final String CREATE_REDSHIFT_CLUSTER = "createRedshiftCluster";
    private static final String CREATE_REDSHIFT_DATA_TABLE = "createRedshiftDataTable";
    private static final String CREATE_REDSHIFT_FILE_TABLE = "createRedshiftFileTable";
    private static final String CREATE_S3_BUCKET = "createS3Bucket";
    private static final String CREATE_ELASTICSEARCH_CLUSTER = "createElasticsearchCluster";
    private static final boolean DEFAULT_CREATE_RESOURCES = false;

    // Create Amazon DynamoDB Resource constants
    private static final String DYNAMODB_KEY = "dynamoDBKey";
    private static final String DYNAMODB_READ_CAPACITY_UNITS = "readCapacityUnits";
    private static final String DYNAMODB_WRITE_CAPACITY_UNITS = "writeCapacityUnits";
    private static final Long DEFAULT_DYNAMODB_READ_CAPACITY_UNITS = 1l;
    private static final Long DEFAULT_DYNAMODB_WRITE_CAPACITY_UNITS = 1l;
    // Create Amazon Redshift Resource constants
    private static final String REDSHIFT_CLUSTER_IDENTIFIER = "redshiftClusterIdentifier";
    private static final String REDSHIFT_DATABASE_NAME = "redshiftDatabaseName";
    private static final String REDSHIFT_CLUSTER_TYPE = "redshiftClusterType";
    private static final String REDSHIFT_NUMBER_OF_NODES = "redshiftNumberOfNodes";
    private static final int DEFAULT_REDSHIFT_NUMBER_OF_NODES = 2;

    // Create Amazon S3 Resource constants
    private static final String S3_BUCKET = "s3Bucket";

    // Elasticsearch Cluster Resource constants
    private static final String EC2_ELASTICSEARCH_FILTER_NAME = "tag:type";
    private static final String EC2_ELASTICSEARCH_FILTER_VALUE = "elasticsearch";

    // Create Stream Source constants
    private static final String CREATE_STREAM_SOURCE = "createStreamSource";
    private static final String LOOP_OVER_STREAM_SOURCE = "loopOverStreamSource";
    private static final boolean DEFAULT_CREATE_STREAM_SOURCE = false;
    private static final boolean DEFAULT_LOOP_OVER_STREAM_SOURCE = false;
    private static final String INPUT_STREAM_FILE = "inputStreamFile";

    // Class variables
    protected final KinesisConnectorConfiguration config;
    private final Properties properties;

    /**
     * Create a new KinesisConnectorExecutor based on the provided configuration (*.propertes) file.
     * 
     * @param configFile
     *        The name of the configuration file to look for on the classpath
     */
    public KinesisConnectorExecutor(String configFile) {
        InputStream configStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);

        if (configStream == null) {
            String msg = "Could not find resource " + configFile + " in the classpath";
            throw new IllegalStateException(msg);
        }
        properties = new Properties();
        try {
            properties.load(configStream);
            configStream.close();
        } catch (IOException e) {
            String msg = "Could not load properties file " + configFile + " from classpath";
            throw new IllegalStateException(msg, e);
        }
        this.config = new KinesisConnectorConfiguration(properties, getAWSCredentialsProvider());
        setupAWSResources();
        setupInputStream();

        // Initialize executor with configurations
        super.initialize(config);
    }

    /**
     * Returns an {@link AWSCredentialsProvider} with the permissions necessary to accomplish all specified
     * tasks. At the minimum it will require read permissions for Amazon Kinesis. Additional read permissions
     * and write permissions may be required based on the Pipeline used.
     * 
     * @return
     */
    public AWSCredentialsProvider getAWSCredentialsProvider() {
        return new DefaultAWSCredentialsProviderChain();
    }

    /**
     * Setup necessary AWS resources for the samples. By default, the Executor does not create any
     * AWS resources. The user must specify true for the specific create properties in the
     * configuration file.
     */
    private void setupAWSResources() {
        if (parseBoolean(CREATE_KINESIS_INPUT_STREAM, DEFAULT_CREATE_RESOURCES, properties)) {
            KinesisUtils.createInputStream(config);
        }

        if (parseBoolean(CREATE_KINESIS_OUTPUT_STREAM, DEFAULT_CREATE_RESOURCES, properties)) {
            KinesisUtils.createOutputStream(config);
        }

        if (parseBoolean(CREATE_DYNAMODB_DATA_TABLE, DEFAULT_CREATE_RESOURCES, properties)) {
            String key = properties.getProperty(DYNAMODB_KEY);
            Long readCapacityUnits =
                    parseLong(DYNAMODB_READ_CAPACITY_UNITS, DEFAULT_DYNAMODB_READ_CAPACITY_UNITS, properties);
            Long writeCapacityUnits =
                    parseLong(DYNAMODB_WRITE_CAPACITY_UNITS, DEFAULT_DYNAMODB_WRITE_CAPACITY_UNITS, properties);
            createDynamoDBTable(key, readCapacityUnits, writeCapacityUnits);
        }

        if (parseBoolean(CREATE_REDSHIFT_CLUSTER, DEFAULT_CREATE_RESOURCES, properties)) {
            String clusterIdentifier = properties.getProperty(REDSHIFT_CLUSTER_IDENTIFIER);
            String databaseName = properties.getProperty(REDSHIFT_DATABASE_NAME);
            String clusterType = properties.getProperty(REDSHIFT_CLUSTER_TYPE);
            int numberOfNodes = parseInt(REDSHIFT_NUMBER_OF_NODES, DEFAULT_REDSHIFT_NUMBER_OF_NODES, properties);
            createRedshiftCluster(clusterIdentifier, databaseName, clusterType, numberOfNodes);
        }

        if (parseBoolean(CREATE_REDSHIFT_DATA_TABLE, DEFAULT_CREATE_RESOURCES, properties)) {
            createRedshiftDataTable();
        }

        if (parseBoolean(CREATE_REDSHIFT_FILE_TABLE, DEFAULT_CREATE_RESOURCES, properties)) {
            createRedshiftFileTable();
        }

        if (parseBoolean(CREATE_S3_BUCKET, DEFAULT_CREATE_RESOURCES, properties)) {
            String s3Bucket = properties.getProperty(S3_BUCKET);
            createS3Bucket(s3Bucket);
        }

        if (parseBoolean(CREATE_ELASTICSEARCH_CLUSTER, DEFAULT_CREATE_RESOURCES, properties)) {
            createElasticsearchCluster();
        }
    }

    /**
     * Helper method to spawn the {@link StreamSource} in a separate thread.
     */
    private void setupInputStream() {
        if (parseBoolean(CREATE_STREAM_SOURCE, DEFAULT_CREATE_STREAM_SOURCE, properties)) {
            String inputFile = properties.getProperty(INPUT_STREAM_FILE);
            StreamSource streamSource;
            if (config.BATCH_RECORDS_IN_PUT_REQUEST) {
                streamSource =
                        new BatchedStreamSource(config, inputFile, parseBoolean(LOOP_OVER_STREAM_SOURCE,
                                DEFAULT_LOOP_OVER_STREAM_SOURCE,
                                properties));

            } else {
                streamSource =
                        new StreamSource(config, inputFile, parseBoolean(LOOP_OVER_STREAM_SOURCE,
                                DEFAULT_LOOP_OVER_STREAM_SOURCE,
                                properties));
            }
            Thread streamSourceThread = new Thread(streamSource);
            LOG.info("Starting stream source.");
            streamSourceThread.start();
        }
    }

    /**
     * Helper method to create the Amazon DynamoDB table.
     * 
     * @param key
     *        The name of the hashkey field in the Amazon DynamoDB table
     * @param readCapacityUnits
     *        Read capacity of the Amazon DynamoDB table
     * @param writeCapacityUnits
     *        Write capacity of the Amazon DynamoDB table
     */
    private void createDynamoDBTable(String key, long readCapacityUnits, long writeCapacityUnits) {
        LOG.info("Creating Amazon DynamoDB table " + config.DYNAMODB_DATA_TABLE_NAME);
        AmazonDynamoDBClient dynamodbClient = new AmazonDynamoDBClient(config.AWS_CREDENTIALS_PROVIDER);
        dynamodbClient.setEndpoint(config.DYNAMODB_ENDPOINT);
        DynamoDBUtils.createTable(dynamodbClient,
                config.DYNAMODB_DATA_TABLE_NAME,
                key,
                readCapacityUnits,
                writeCapacityUnits);
    }

    /**
     * Helper method to create the Amazon Redshift cluster.
     * 
     * @param clusterIdentifier
     *        Unique identifier for the name of the Amazon Redshift cluster
     * @param databaseName
     *        Name for the database in the Amazon Redshift cluster
     * @param clusterType
     *        dw.hs1.xlarge or dw.hs1.8xlarge
     * @param numberOfNodes
     *        Number of nodes for the Amazon Redshift cluster
     */
    private void createRedshiftCluster(String clusterIdentifier,
            String databaseName,
            String clusterType,
            int numberOfNodes) {
        // Make sure the Amazon Redshift cluster is available
        AmazonRedshiftClient redshiftClient = new AmazonRedshiftClient(config.AWS_CREDENTIALS_PROVIDER);
        redshiftClient.setEndpoint(config.REDSHIFT_ENDPOINT);
        LOG.info("Creating Amazon Redshift cluster " + clusterIdentifier);
        RedshiftUtils.createCluster(redshiftClient,
                clusterIdentifier,
                databaseName,
                config.REDSHIFT_USERNAME,
                config.REDSHIFT_PASSWORD,
                clusterType,
                numberOfNodes);

        // Update the Amazon Redshift connection url
        config.REDSHIFT_URL = RedshiftUtils.getClusterURL(redshiftClient, clusterIdentifier);
    }

    /**
     * Helper method to create the data table in Amazon Redshift.
     */
    private void createRedshiftDataTable() {
        Properties p = new Properties();
        p.setProperty("user", config.REDSHIFT_USERNAME);
        p.setProperty("password", config.REDSHIFT_PASSWORD);
        if (RedshiftUtils.tableExists(p, config.REDSHIFT_URL, config.REDSHIFT_DATA_TABLE)) {
            LOG.info("Amazon Redshift data table " + config.REDSHIFT_DATA_TABLE + " exists.");
            return;
        }
        try {
            LOG.info("Creating Amazon Redshift data table " + config.REDSHIFT_DATA_TABLE);
            RedshiftUtils.createRedshiftTable(config.REDSHIFT_URL,
                    p,
                    config.REDSHIFT_DATA_TABLE,
                    getKinesisMessageModelFields());
        } catch (SQLException e) {
            String msg = "Could not create Amazon Redshift data table " + config.REDSHIFT_DATA_TABLE;
            throw new IllegalStateException(msg, e);
        }
    }

    /**
     * Helper method to create the file table in Amazon Redshift.
     */
    private void createRedshiftFileTable() {
        Properties p = new Properties();
        p.setProperty("user", config.REDSHIFT_USERNAME);
        p.setProperty("password", config.REDSHIFT_PASSWORD);
        if (RedshiftUtils.tableExists(p, config.REDSHIFT_URL, config.REDSHIFT_FILE_TABLE)) {
            LOG.info("Amazon Redshift file table " + config.REDSHIFT_FILE_TABLE + " exists.");
            return;
        }
        try {
            LOG.info("Creating Amazon Redshift file table " + config.REDSHIFT_FILE_TABLE);
            RedshiftUtils.createRedshiftTable(config.REDSHIFT_URL, p, config.REDSHIFT_FILE_TABLE, getFileTableFields());
        } catch (SQLException e) {
            String msg = "Could not create Amazon Redshift file table " + config.REDSHIFT_FILE_TABLE;
            throw new IllegalStateException(msg, e);
        }
    }

    /**
     * Helper method to build the data table.
     * 
     * @return Fields for the data table
     */
    private static List<String> getKinesisMessageModelFields() {
        List<String> fields = new ArrayList<String>();
        fields.add("userid integer not null distkey sortkey");
        fields.add("username char(8)");
        fields.add("firstname varchar(30)");
        fields.add("lastname varchar(30)");
        fields.add("city varchar(30)");
        fields.add("state char(2)");
        fields.add("email varchar(100)");
        fields.add("phone char(14)");
        fields.add("likesports boolean");
        fields.add("liketheatre boolean");
        fields.add("likeconcerts boolean");
        fields.add("likejazz boolean");
        fields.add("likeclassical boolean");
        fields.add("likeopera boolean");
        fields.add("likerock boolean");
        fields.add("likevegas boolean");
        fields.add("likebroadway boolean");
        fields.add("likemusicals boolean");
        return fields;
    }

    /**
     * Helper method to help create the file table.
     * 
     * @return File table fields
     */
    private List<String> getFileTableFields() {
        List<String> fields = new ArrayList<String>();
        fields.add(config.REDSHIFT_FILE_KEY_COLUMN + " varchar(255) primary key");
        return fields;
    }

    /**
     * Helper method to create the Amazon S3 bucket.
     * 
     * @param s3Bucket
     *        The name of the bucket to create
     */
    private void createS3Bucket(String s3Bucket) {
        AmazonS3Client client = new AmazonS3Client(config.AWS_CREDENTIALS_PROVIDER);
        client.setEndpoint(config.S3_ENDPOINT);
        LOG.info("Creating Amazon S3 bucket " + s3Bucket);
        S3Utils.createBucket(client, s3Bucket);
    }

    /**
     * Helper method to create Elasticsearch cluster at set correct endpoint.
     */
    private void createElasticsearchCluster() {
        // Create stack if not already up
        AmazonCloudFormation cloudFormationClient = new AmazonCloudFormationClient(config.AWS_CREDENTIALS_PROVIDER);
        cloudFormationClient.setRegion(RegionUtils.getRegion(config.REGION_NAME));
        CloudFormationUtils.createStackIfNotExists(cloudFormationClient, config);

        // Update the elasticsearch endpoint to use endpoint in created cluster
        AmazonEC2 ec2Client = new AmazonEC2Client(config.AWS_CREDENTIALS_PROVIDER);
        ec2Client.setRegion(RegionUtils.getRegion(config.REGION_NAME));
        config.ELASTICSEARCH_ENDPOINT =
                EC2Utils.getEndpointForFirstActiveInstanceWithTag(ec2Client,
                        EC2_ELASTICSEARCH_FILTER_NAME,
                        EC2_ELASTICSEARCH_FILTER_VALUE);
        if (config.ELASTICSEARCH_ENDPOINT == null || config.ELASTICSEARCH_ENDPOINT.isEmpty()) {
            throw new RuntimeException("Could not find active Elasticsearch endpoint from cluster.");
        }
    }

    /**
     * Helper method used to parse boolean properties.
     * 
     * @param property
     *        The String key for the property
     * @param defaultValue
     *        The default value for the boolean property
     * @param properties
     *        The properties file to get property from
     * @return property from property file, or if it is not specified, the default value
     */
    private static boolean parseBoolean(String property, boolean defaultValue, Properties properties) {
        return Boolean.parseBoolean(properties.getProperty(property, Boolean.toString(defaultValue)));
    }

    /**
     * Helper method used to parse long properties.
     * 
     * @param property
     *        The String key for the property
     * @param defaultValue
     *        The default value for the long property
     * @param properties
     *        The properties file to get property from
     * @return property from property file, or if it is not specified, the default value
     */
    private static long parseLong(String property, long defaultValue, Properties properties) {
        return Long.parseLong(properties.getProperty(property, Long.toString(defaultValue)));
    }

    /**
     * Helper method used to parse integer properties.
     * 
     * @param property
     *        The String key for the property
     * @param defaultValue
     *        The default value for the integer property
     * @param properties
     *        The properties file to get property from
     * @return property from property file, or if it is not specified, the default value
     */
    private static int parseInt(String property, int defaultValue, Properties properties) {
        return Integer.parseInt(properties.getProperty(property, Integer.toString(defaultValue)));
    }
}
