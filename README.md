# Amazon Kinesis Connector Library

The **Amazon Kinesis Connector Library** helps Java developers integrate [Amazon Kinesis][aws-kinesis] with other AWS and non-AWS services. The current version of the library provides connectors for [Amazon DynamoDB][aws-dynamodb], [Amazon Redshift][aws-redshift], [Amazon S3][aws-s3], [Elasticsearch][Elasticsearch]. The library also includes [sample connectors](#samples) of each type, plus Apache Ant build files for running the samples.

## Requirements

 + **Amazon Kinesis Client Library**: In order to use the Amazon Kinesis Connector Library, you'll also need the [Amazon Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client). 
 + **Java 1.7**: The Amazon Kinesis Client Library requires [Java 1.7 (Java SE 7)](http://www.oracle.com/technetwork/java/javase/overview/index.html) or later.
 + **Elasticsearch 1.2.1**: The Elasticsearch connector depends on [Elasticsearch 1.2.1][Elasticsearch].
 + **SQL driver** (Amazon Redshift only): If you're using an Amazon Redshift connector, you'll need a driver that will allow your SQL client to connect to your Amazon Redshift cluster. For more information, see [Download the Client Tools and the Drivers](http://docs.aws.amazon.com/redshift/latest/gsg/before-you-begin.html#getting-started-download-tools) in the Amazon Redshift Getting Started Guide.

## Overview

Each Amazon Kinesis connector application is a pipeline that determines how records from an Amazon Kinesis stream will be handled. Records are retrieved from the stream, transformed according to a user-defined data model, buffered for batch processing, and then emitted to the appropriate AWS service.

A connector pipeline uses the following interfaces:

+ **IKinesisConnectorPipeline**: The pipeline implementation itself.
+ **ITransformer**: Defines the transformation of records from the Amazon Kinesis stream in order to suit the user-defined data model. Includes methods for custom serializer/deserializers.
+ **IFilter**: IFilter defines a method for excluding irrelevant records from the processing.
+ **IBuffer**: IBuffer defines a system for batching the set of records to be processed. The application can specify three thresholds: number of records, total byte count, and time. When one of these thresholds is crossed, the buffer is flushed and the data is emitted to the destination.
+ **IEmitter**: Defines a method that makes client calls to other AWS services and persists the records stored in the buffer. The records can also be sent to another Amazon Kinesis stream.

Each connector depends on the implementation of KinesisConnectorRecordProcessor to manage the pipeline. The KinesisConnectorRecordProcessor class implements the IRecordProcessor interface in the [Amazon Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client/).

## Implementation Highlights

The library includes implementations for use with [Amazon DynamoDB][aws-dynamodb], [Amazon Redshift][aws-redshift], [Amazon S3][aws-s3], and [Elasticsearch][Elasticsearch]. This section provides a few notes about each connector type. For full details, see the [samples](#samples) and the Javadoc.

### kinesis.connectors.dynamodb

+ **DynamoDBTransformer**: Implement the fromClass method to map your data model to a format that's compatible with the AmazonDynamoDB client (Map&lt;String,AttributeValue&gt;). 
+ For more information on Amazon DynamoDB formats and putting items, see [Working with Items Using the AWS SDK for Java Low-Level API](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LowLevelJavaItemCRUD.html#PutLowLevelAPIJava) in the Amazon DynamoDB Developer Guide.

### kinesis.connectors.redshift

+ **RedshiftTransformer**: Implement the toDelimitedString method to output a delimited-string representation of your data model. The string must be compatible with an [Amazon Redshift COPY command](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html#r_COPY-copy-from-amazon-s3-synopsis).
+ For more information about Amazon Redshift copy operations and manifests, see [COPY](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html#r_COPY-copy-from-amazon-s3-synopsis) and [Using a manifest to specify data files](http://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html) in the Amazon Redshift Developer Guide.

### kinesis.connectors.s3 

+ **S3Emitter**: This class writes the buffer contents to a single file in Amazon S3. The file name is determined by the Amazon Kinesis sequence numbers of the first and last records in the buffer. For more information about sequence numbers, see [Add Data to a Stream](http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-api-java.html#kinesis-using-api-java-add-data-to-stream) in the Amazon Kinesis Developer Guide.

### kinesis.connectors.elasticsearch

+ **KinesisMessageModelElasticsearchTransformer**: This class provides an implementation for fromClass by transforming the record into JSON format and setting the index, type, and id to use for Elasticsearch.
+ **BatchedKinesisMessageModelElasticsearchTransformer**: This class extends KinesisMessageModelElasticsearchTransformer. If you batch events before putting data into Kinesis, this class will help you unpack the events before loading them into Elasticsearch.

 
## Configuration

Set the following variables (common to all connector types) in kinesis.connectors.KinesisConnectorConfiguration:

+ **AWSCredentialsProvider**: Specify the implementation of [AWSCredentialsProvider](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html) that supplies your [AWS credentials](http://docs.aws.amazon.com/general/latest/gr/aws-security-credentials.html).
+ **APP_NAME**: The Amazon Kinesis application name (*not* the connector application name) for use with kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration. For more information, see [Developing Record Consumer Applications](http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-app.html) in the Amazon Kinesis Developer Guide.
+ **KINESIS_ENDPOINT** and **KINESIS_INPUT_STREAM**: The endpoint and name of the Kinesis stream that contains the data you're connecting to other AWS services.

Service-specific configuration variables are set in the respective emitter implementations (e.g., kinesis.connectors.dynamodb.DynamoDBEmitter).

## Samples

The **samples** folder contains common classes for all the samples. The subfolders contain implementations of the pipeline and executor classes, along with Apache Ant build.xml files for running the samples. 

Each sample uses the following files:

+ **StreamSource.java**: A simple application that sends records to an Amazon Kinesis stream.
+ **users.txt**: JSON records that are parsed line by line by the StreamSource program; the basis of KinesisMessageModel.
+ **KinesisMessageModel.java**: The data model for the users.txt records.
+ **KinesisConnectorExecutor.java**: An abstract implementation of an Amazon Kinesis connector application, which includes these features:
	+ Configures the constructor, using the samples.utils package and the .properties file in the sample subfolder.
	+ Provides the getKinesisConnectorRecordProcessorFactory() method, which is implemented by the executors in the sample subfolders; each executor returns an instance of a factory configured with the appropriate pipeline. 
	+ Provides a run() method for spawning a worker thread that uses the result of getKinesisConnectorRecordProcessorFactory(). 
+ **.properties**: The service-specific key-value properties for configuring the connector. 
+ **&lt;service/type&gt;Pipeline**: The implementation of IKinesisConnectorPipeline for the sample. Each pipeline class returns a service-specific transformer and emitter, as well as simple buffer and filter implementations (BasicMemoryBuffer and AllPassFilter).

## Running a Sample

To run a sample, complete these steps:

1. Edit the *.properties file, adding your AWS credentials and any necessary AWS resource configurations.
	+ **Note:** In the samples, [KinesisConnectorExecutor](https://github.com/awslabs/amazon-kinesis-connectors/blob/master/src/main/samples/KinesisConnectorExecutor.java) uses the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html), which looks for credentials supplied by environment variables, system properties, or IAM role on Amazon EC2. If you prefer to specify your AWS credentials via a properties file on the classpath, edit the sample code to use [ClasspathPropertiesFileCredentialsProvider](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/ClasspathPropertiesFileCredentialsProvider.html) instead.
2. Confirm that the required AWS resources exist, or set the flags in the *.properties file to indicate that resources should be created when the sample is run.
3. Build the samples using Maven
   ```
   cd samples
   mvn package
   ```
4. Scripts to start each of the samples will be available in `target/appassembler/bin`

## Release Notes
### Release 1.3.0 (November 17, 2016)
* Upgraded the Amazon Kinesis Client Library to version 1.7.2.
* Upgraded the AWS Java SDK to 1.11.14.
* Migrated the sample to now use Maven for dependency management, and execution.
* **Maven Artifact Signing Change** 
  * Artifacts are now signed by the identity `Amazon Kinesis Tools <amazon-kinesis-tools@amazon.com>`

### Release 1.2.0 (June 23, 2015)
+ Upgraded KCL to 1.4.0
+ Added pipelined record processor that decouples Amazon Kinesis GetRecords() and IRecordProcessor's ProcessRecords() API calls for efficiency.

### Release 1.1.2 (May 27, 2015)
+ Upgraded AWS SDK to 1.9, KCL to 1.3.0
+ Added pom.xml file

### Release 1.1.1 (Sep 11, 2014)
+ Added connector to Elasticsearch

### Release 1.1 (June 30, 2014)
+ Added time threshold to IBuffer
+ Added region name support

## Related Resources

[Amazon Kinesis Developer Guide](http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html)  
[Amazon Kinesis API Reference](http://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html)  

[Amazon DynamoDB Developer Guide](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html)   
[Amazon DynamoDB API Reference](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/)

[Amazon Redshift Documentation](http://aws.amazon.com/documentation/redshift/)

[Amazon S3 Documentation and Videos](http://aws.amazon.com/documentation/s3/)

[Elasticsearch](http://www.elasticsearch.org/)

[aws-kinesis]: http://aws.amazon.com/kinesis/
[aws-dynamodb]: http://aws.amazon.com/dynamodb/
[aws-redshift]: http://aws.amazon.com/redshift/
[aws-s3]: http://aws.amazon.com/s3/
[Elasticsearch]: http://www.elasticsearch.org/

## License

This library is licensed under the Apache 2.0 License.
