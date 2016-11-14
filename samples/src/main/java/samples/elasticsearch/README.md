# Amazon Kinesis Connector to Elasticsearch

The **Amazon Kinesis Connector to Elasticsearch** is a pipeline to send data from Amazon Kinesis to the popular open source library [Elasticsearch][Elasticsearch].

Elasticsearch is an Open Source, Distributed, RESTful Search Engine. It is built on top of [Apache Lucene][lucene] but strives to make full text search easy by hiding the complexities of Lucene behind a simple, coherent API. For more information, check out the [Elasticsearch website][Elasticsearch] and read the [Elasticsearch Getting Started Guide][elasticsearch-getting-started].

## Features

By using the Amazon Kinesis Connector to Elasticsearch, you can:

+ Filter and transform Amazon Kinesis records into a format compatible with Elasticsearch
+ Provide indexing information on a per record basis (index, type, id, create, version, ttl, source)
+ Configure the Elasticsearch TransportClient through a properties file (hostname, port, client sniffing, ping timeouts, etc)
+ Efficiently pass data to Elasticsearch using the Bulk API
+ Automatic cluster health checks and retry behavior on failures
+ Easily create elasticsearch clusters using the provided Amazon CloudFormation template

Furthermore, this connector can easily run within an Elasticsearch cluster by building it into an [Elasticsearch River](http://www.elasticsearch.org/guide/en/elasticsearch/rivers/current/).

## Getting Started

If you haven't already, check out the main repository's [Readme][kinesis-connectors] (especially the [Requirements](https://github.com/awslabs/amazon-kinesis-connectors#requirements) and [Configuration](https://github.com/awslabs/amazon-kinesis-connectors#configuration) sections).

### Additional Dependencies

You can automatically setup the project directory using the command:

````ant setup````

This will call the ````download```` and ````unzip```` targets of the build file and place required dependencies in your classpath. Alternatively, you can find the required jars on  [maven](http://mvnrepository.com/artifact/org.elasticsearch/elasticsearch).

### Configuration

Edit ElasticsearchSample.properties to control application behavior, including creating resources if they do not exist, setting up input data, and configuring endpoints.

### Using the Amazon CloudFormation Template

An Amazon CloudFormation template is included to facilitate creating an Elasticsearch cluster on Amazon EC2. It creates an [Amazon AutoScaling][autoscaling] group which utilizes the [Elasticsearch Cloud AWS Plugin][elasticsearch-cloud-plugin] to form a cluster that each instance will automatically join. Please note, the Amazon CloudFormation stack also creates an [IAM Role][iam-role] to allow the application to authenticate your account without the need for you to provide explicit credentials. See [Using IAM Roles for EC2 Instances with the SDK for Java][iam-roles-java-sdk] for more information.

The template has the following parameters that must be set in ElasticsearchSample.properties:

1. InstanceType: The type of Amazon EC2 instance to create.  See [EC2 Instance Types][ec2-instance-types] for more information about instance types.
1. ClusterSize: The number of Amazon EC2 instances to create.
1. KeyName: The name of an existing key pair to use for SSH access to instances.
1. SSHLocation: The range of IP addresses with access to SSH into the instances. 
1. ElasticsearchVersion: The version of Elasticsearch to install on each instance. Make sure it is the same version as the jar included in your classpath.

Furthermore, you can opt out of using the template and set up your own cluster by setting ```createElasticsearchCluster = false``` in ElasticsearchSample.properties.

The template can be found at ```src/main/samples/elasticsearch/Elasticsearch.template```. Visit the [AWS CloudFormation][cloudformation] page for more information about what Amazon CloudFormation is and how you can leverage it to create and manage AWS resources.

### Running the Sample Application

Similar to the other connectors, use the included Apache Ant build file. Within this sample directory, execute:

```ant run```

### Cleaning Up

After terminating the application, you should delete all AWS resources to avoid getting billed. If you used the default names, there will be

+ an Amazon Kinesis stream named ````elasticsearchStream````
+ an Amazon DynamoDB table named ````kinesisToElasticsearch```` 
+ an Amazon CloudFormation stack named ````kinesisElasticsearchSample````

You can delete these resources using the [AWS Console][aws-console], or the [AWS CLI][aws-cli] with these commands (assuming region us-east-1):

+ ````aws --region us-east-1 dynamodb delete-table --table-name kinesisToElasticsearch````
+ ````aws --region us-east-1 kinesis delete-stream --stream-name elasticsearchStream````
+ ````aws --region us-east-1 cloudformation delete-stack --stack-name kinesisElasticsearchSample````


## Recommended Resources

[Amazon Kinesis Developer Guide](http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html)  
[Amazon Kinesis API Reference](http://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html)  

[Elasticsearch Getting Started Guide](elasticsearch-getting-started)  
[Elasticsearch Cloud AWS Plugin](elasticsearch-cloud-plugin)

[Amazon CloudFormation Developer Guide](http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/CHAP_Intro.html)  

[aws-kinesis]: http://aws.amazon.com/kinesis/
[aws-dynamodb]: http://aws.amazon.com/dynamodb/
[aws-redshift]: http://aws.amazon.com/redshift/
[aws-s3]: http://aws.amazon.com/s3/
[aws-cloudformation]: http://aws.amazon.com/cloudformation/
[aws-cli]:http://aws.amazon.com/cli/
[aws-console]:http://aws.amazon.com/console/
[kinesis]: http://aws.amazon.com/kinesis
[kcl]: https://github.com/awslabs/amazon-kinesis-client
[kinesis-connectors]: https://github.com/awslabs/amazon-kinesis-connectors
[kinesis-forum]: http://developer.amazonwebservices.com/connect/forum.jspa?forumID=169
[docs-signup]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html
[kinesis-guide]: http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html
[kinesis-getting-started]: http://docs.aws.amazon.com/kinesis/latest/dev/getting-started.html
[kinesis-guide-begin]: http://docs.aws.amazon.com/kinesis/latest/dev/before-you-begin.html
[kinesis-guide-create]: http://docs.aws.amazon.com/kinesis/latest/dev/step-one-create-stream.html
[kinesis-guide-applications]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-app.html
[autoscaling]: http://aws.amazon.com/autoscaling/
[cloudformation]: http://aws.amazon.com/cloudformation
[ec2-instance-types]: http://aws.amazon.com/ec2/instance-types
[iam-role]: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html
[iam-roles-java-sdk]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-roles.html
[maven]: http://maven.apache.org/
[elasticsearch]: http://www.elasticsearch.org/
[elasticsearch-getting-started]: http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/getting-started.html
[elasticsearch-cloud-plugin]: https://github.com/elasticsearch/elasticsearch-cloud-aws
[lucene]: http://lucene.apache.org/