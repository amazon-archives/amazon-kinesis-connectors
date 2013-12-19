/*
 * Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.connectors.dynamodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;

/**
 * This class is used to store records from a stream in a DynamoDB table. It requires the use of a
 * DynamoDBTransformer, which is able to transform records into a format that can be sent to
 * DynamoDB. A DynamoDB client is used to perform batch requests on the contents of a buffer when
 * emitting. This class requires the configuration of a DynamoDB endpoint and table name.
 */
public class DynamoDBEmitter implements IEmitter<Map<String, AttributeValue>> {
    private static final Log LOG = LogFactory.getLog(DynamoDBEmitter.class);
    protected final String dynamoDBEndpoint;
    protected final String dynamoDBTableName;
    protected final AmazonDynamoDBClient dynamoDBClient;

    public DynamoDBEmitter(KinesisConnectorConfiguration configuration) {
        // DynamoDB Config
        this.dynamoDBEndpoint = configuration.DYNAMODB_ENDPOINT;
        this.dynamoDBTableName = configuration.DYNAMODB_DATA_TABLE_NAME;
        // Client
        this.dynamoDBClient = new AmazonDynamoDBClient(configuration.AWS_CREDENTIALS_PROVIDER);
        this.dynamoDBClient.setEndpoint(this.dynamoDBEndpoint);
    }

    @Override
    public List<Map<String, AttributeValue>> emit(final UnmodifiableBuffer<Map<String, AttributeValue>> buffer)
            throws IOException {
        // Map of WriteRequests to records for reference
        Map<WriteRequest, Map<String, AttributeValue>> requestMap = new HashMap<WriteRequest, Map<String, AttributeValue>>();
        List<Map<String, AttributeValue>> unproc = new ArrayList<Map<String, AttributeValue>>();
        // Build a batch request with a record list
        List<WriteRequest> rList = new ArrayList<WriteRequest>();
        List<Map<String, AttributeValue>> resultList;
        // DynamoDB only allows one operation per item in a bulk insertion (no duplicate items)
        Set<Map<String, AttributeValue>> uniqueItems = uniqueItems(buffer.getRecords());
        for (Map<String, AttributeValue> item : uniqueItems) {
            WriteRequest wr = new WriteRequest().withPutRequest(new PutRequest().withItem(item));
            // add to the map
            requestMap.put(wr, item);
            // add to the list of requests
            rList.add(wr);
            // Max of sixteen not to exceed maximum request size
            if (rList.size() == 16) {
                resultList = performBatchRequest(rList, requestMap);
                unproc.addAll(resultList);
                rList.clear();
            }
        }
        resultList = performBatchRequest(rList, requestMap);
        unproc.addAll(resultList);
        LOG.info("Successfully emitted " + (buffer.getRecords().size() - unproc.size())
                + " records into DynamoDB.");
        return unproc;
    }

    @Override
    public void fail(List<Map<String, AttributeValue>> records) {
        for (Map<String, AttributeValue> record : records) {
            LOG.error("Could not emit record: " + record);
        }
    }

    /**
     * This method performs a batch request into DynamoDB and returns records that were
     * unsuccessfully processed by the batch request. Throws IOException if the client calls to
     * DynamoDB encounter an exception.
     * 
     * @param rList
     *            list of WriteRequests to batch
     * @param requestMap
     *            map of WriteRequests to records
     * @return records that did not get put in the table by the batch request
     * @throws IOException
     *             if DynamoDB client encounters an exception
     */
    private List<Map<String, AttributeValue>> performBatchRequest(List<WriteRequest> rList,
            Map<WriteRequest, Map<String, AttributeValue>> requestMap) throws IOException {
        // Requests in the batch
        Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();

        if (rList.isEmpty()) {
            return Collections.emptyList();
        }
        requestItems.put(dynamoDBTableName, rList);
        BatchWriteItemResult result;
        BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest()
                .withRequestItems(requestItems);
        try {
            result = dynamoDBClient.batchWriteItem(batchWriteItemRequest);
            return unproccessedItems(result, requestMap);
        } catch (AmazonClientException e) {
            String message = "DynamoDB Client could not perform batch request";
            LOG.error(message, e);
            throw new IOException(message, e);
        } catch (Exception e) {
            String message = "Unexpected Exception while performing batch request";
            LOG.error(message, e);
            throw new IOException(message, e);
        }
    }

    private List<Map<String, AttributeValue>> unproccessedItems(BatchWriteItemResult result,
            Map<WriteRequest, Map<String, AttributeValue>> requestMap) {
        Collection<List<WriteRequest>> items = result.getUnprocessedItems().values();
        List<Map<String, AttributeValue>> unprocessed = new ArrayList<Map<String, AttributeValue>>();
        // retrieve the unprocessed items
        for (List<WriteRequest> list : items) {
            for (WriteRequest request : list) {
                unprocessed.add(requestMap.get(request));
            }
        }

        return unprocessed;
    }

    /**
     * This helper method is used to dedupe a list of items. Use this method to dedupe the contents
     * of a buffer before performing a DynamoDB batch write request.
     * 
     * @param items
     *            a list of Map<String,AttributeValue> items
     * @return the subset of unique items
     */
    public Set<Map<String, AttributeValue>> uniqueItems(List<Map<String, AttributeValue>> items) {
        return new HashSet<Map<String, AttributeValue>>(items);
    }

    @Override
    public void shutdown() {
        dynamoDBClient.shutdown();
    }
}
