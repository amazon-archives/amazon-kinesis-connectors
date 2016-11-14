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
package samples.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.model.CreateStackRequest;
import com.amazonaws.services.cloudformation.model.DescribeStacksRequest;
import com.amazonaws.services.cloudformation.model.Parameter;
import com.amazonaws.services.cloudformation.model.StackStatus;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;

public class CloudFormationUtils {
    private static Log LOG = LogFactory.getLog(CloudFormationUtils.class);

    public static void createStackIfNotExists(AmazonCloudFormation client, KinesisConnectorConfiguration config) {
        String stackName = config.ELASTICSEARCH_CLOUDFORMATION_STACK_NAME;

        if (stackExists(client, stackName)) {
            StackStatus status = stackStatus(client, stackName);
            switch (status) {
                case CREATE_COMPLETE:
                case UPDATE_COMPLETE:
                    LOG.info("Stack " + stackName + " already exists.");
                    return;
                case CREATE_IN_PROGRESS:
                case UPDATE_IN_PROGRESS:
                case UPDATE_COMPLETE_CLEANUP_IN_PROGRESS:
                    LOG.info("Stack " + stackName + " exists with status: " + status + ". Waiting for completion.");
                    break;
                default:
                    throw new IllegalStateException("Stack " + stackName + " exists but has an invalid status: "
                            + status);
            }
        } else {
            CreateStackRequest createStackRequest = new CreateStackRequest();
            createStackRequest.setStackName(stackName);

            String templateBody = null;
            try {
                templateBody = loadTemplate(config.ELASTICSEARCH_CLOUDFORMATION_TEMPLATE_URL);
            } catch (IOException ioe) {
                LOG.error("Error reading template", ioe);
                throw new RuntimeException("Could not load template at location: "
                        + config.ELASTICSEARCH_CLOUDFORMATION_TEMPLATE_URL);
            }
            createStackRequest.setTemplateBody(templateBody);

            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter().withParameterKey("KeyName")
                    .withParameterValue(config.ELASTICSEARCH_CLOUDFORMATION_KEY_PAIR_NAME));
            parameters.add(new Parameter().withParameterKey("InstanceType")
                    .withParameterValue(config.ELASTICSEARCH_CLOUDFORMATION_CLUSTER_INSTANCE_TYPE));
            parameters.add(new Parameter().withParameterKey("SSHLocation")
                    .withParameterValue(config.ELASTICSEARCH_CLOUDFORMATION_SSH_LOCATION));
            parameters.add(new Parameter().withParameterKey("ClusterSize")
                    .withParameterValue(config.ELASTICSEARCH_CLOUDFORMATION_CLUSTER_SIZE));
            parameters.add(new Parameter().withParameterKey("ElasticsearchVersion")
                    .withParameterValue(config.ELASTICSEARCH_VERSION_NUMBER));
            createStackRequest.setParameters(parameters);

            List<String> capabilities = new ArrayList<String>();
            capabilities.add("CAPABILITY_IAM");
            createStackRequest.setCapabilities(capabilities);

            client.createStack(createStackRequest);
            LOG.info("Stack " + stackName + " is creating");
        }

        // now wait for good status
        while (true) {
            try {
                Thread.sleep(1000 * 10);
            } catch (Exception e) {
            }
            StackStatus status = stackStatus(client, stackName);
            switch (status) {
                case CREATE_COMPLETE:
                case UPDATE_COMPLETE:
                    return;
                case CREATE_IN_PROGRESS:
                case UPDATE_IN_PROGRESS:
                case UPDATE_COMPLETE_CLEANUP_IN_PROGRESS:
                    break;
                default:
                    throw new IllegalStateException("Stack " + stackName + " failed to create with status: " + status);
            }
        }
    }

    private static String loadTemplate(String templateUrl) throws IOException {
        try (InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(templateUrl)) {
            if (input == null) {
                throw new IOException("Could not find template at location: " + templateUrl);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
                StringBuilder stringBuilder = new StringBuilder();
                String line = null;
                while ((line = reader.readLine()) != null) {
                    stringBuilder.append(line).append("\n");
                }
                return stringBuilder.toString();
            }
        }
    }

    private static boolean stackExists(AmazonCloudFormation client, String stackName) {
        DescribeStacksRequest describeStacksRequest = new DescribeStacksRequest();
        describeStacksRequest.setStackName(stackName);
        try {
            client.describeStacks(describeStacksRequest);
            return true;
        } catch (AmazonServiceException e) {
            return false;
        }
    }

    private static StackStatus stackStatus(AmazonCloudFormation client, String stackName) {
        DescribeStacksRequest describeStacksRequest = new DescribeStacksRequest();
        describeStacksRequest.setStackName(stackName);
        // describeStacks (with stack name specified) will return list of size 1 if found
        // and throw AmazonServiceException if no stack with that name exists
        try {
            return StackStatus.fromValue(client.describeStacks(describeStacksRequest)
                    .getStacks()
                    .get(0)
                    .getStackStatus());
        } catch (AmazonServiceException ase) {
            return null;
        }
    }
}
