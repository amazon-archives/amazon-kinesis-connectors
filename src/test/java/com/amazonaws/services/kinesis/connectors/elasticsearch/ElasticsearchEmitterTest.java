/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.easymock.EasyMock;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;

public class ElasticsearchEmitterTest {

    ElasticsearchEmitter emitter;

    KinesisConnectorConfiguration configuration;
    TransportClient elasticsearchClientMock;
    UnmodifiableBuffer<ElasticsearchObject> buffer;
    BulkRequestBuilder mockBulkBuilder;
    IndexRequestBuilder mockIndexBuilder;
    ListenableActionFuture mockFuture;
    BulkResponse mockBulkResponse;

    @Before
    public void setUp() {
        // object under test
        Properties props = new Properties();
        AWSCredentialsProvider creds = createMock(AWSCredentialsProvider.class);
        configuration = new KinesisConnectorConfiguration(props, creds);

        emitter = new ElasticsearchEmitter(configuration);

        buffer = createMock(UnmodifiableBuffer.class);
        mockBulkBuilder = createMock(BulkRequestBuilder.class);
        mockIndexBuilder = createMock(IndexRequestBuilder.class);
        mockFuture = createMock(ListenableActionFuture.class);
        mockBulkResponse = createMock(BulkResponse.class);

        // overwrite the elasticseach client with a mock
        elasticsearchClientMock = createMock(TransportClient.class);

        setField(emitter, "elasticsearchClient", elasticsearchClientMock);
        // overwrite the default backoff time with 0 seconds to speed up tests
        setField(emitter, "BACKOFF_PERIOD", 0);
    }

    private void setField(Object target, String fieldName, Object value) {
        try {
            Class<?> clazz = target.getClass();
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Verify that every object passed to ElasticsearchEmitter gets touched on every required method.
     * 
     * @param index
     * @param type
     * @param id
     * @param source
     * @return
     */
    private ElasticsearchObject createMockRecordAndSetExpectations(String index, String type, String id,
            String source) {
        ElasticsearchObject obj = createMock(ElasticsearchObject.class);
        expect(obj.getIndex()).andReturn(index);
        expect(obj.getType()).andReturn(type);
        expect(obj.getId()).andReturn(id);
        expect(obj.getSource()).andReturn(source);
        expect(obj.getVersion()).andReturn(null);
        expect(obj.getTtl()).andReturn(null);
        expect(obj.getCreate()).andReturn(null);
        return obj;
    }

    /**
     * Building the mock request is the same for every test. The only thing to change is the list of records, which can
     * contain all good records or some corrupt ones.
     * 
     * @param records
     *            the list of records
     */
    private void mockBuildingRequest(List<ElasticsearchObject> records) {
        expect(buffer.getRecords()).andReturn(records);
        expect(elasticsearchClientMock.prepareBulk()).andReturn(mockBulkBuilder);
        if (!records.isEmpty()) {
            expect(elasticsearchClientMock.prepareIndex(EasyMock.anyObject(String.class),
                    EasyMock.anyObject(String.class), EasyMock.anyObject(String.class))).andReturn(mockIndexBuilder);
            expectLastCall().times(records.size());
            expect(mockIndexBuilder.setSource(EasyMock.anyObject(String.class))).andReturn(mockIndexBuilder);
            expectLastCall().times(records.size());
            expect(mockBulkBuilder.add(mockIndexBuilder)).andReturn(mockBulkBuilder);
            expectLastCall().times(records.size());
        }
    }

    private void mockBuildingAndExecutingRequest(List<ElasticsearchObject> records) {
        mockBuildingRequest(records);
        expect(mockBulkBuilder.execute()).andReturn(mockFuture);
        expect(mockFuture.actionGet()).andReturn(mockBulkResponse);
    }

    private void verifyRecords(List<ElasticsearchObject> records) {
        for (ElasticsearchObject record : records) {
            verify(record);
        }
    }

    private void verifyResponses(BulkItemResponse[] responses) {
        for (int i = 0; i < responses.length; i++) {
            verify(responses[i]);
        }
    }

    @Test
    public void testNoRecordsCase() throws IOException {

        List<ElasticsearchObject> records = new ArrayList<ElasticsearchObject>();

        expect(buffer.getRecords()).andReturn(records);

        replay(elasticsearchClientMock, buffer, mockBulkBuilder, mockIndexBuilder, mockFuture, mockBulkResponse);

        List<ElasticsearchObject> failures = emitter.emit(buffer);
        assertTrue(failures.isEmpty());

        verify(elasticsearchClientMock, buffer, mockBulkBuilder, mockIndexBuilder, mockFuture, mockBulkResponse);
    }

    @Test
    public void testWorkingCaseNoFailures() throws IOException {

        List<ElasticsearchObject> records = new ArrayList<ElasticsearchObject>();
        ElasticsearchObject r1 = createMockRecordAndSetExpectations("sample-index", "type", "1", "{\"name\":\"Mike\"}");
        records.add(r1);
        ElasticsearchObject r2 = createMockRecordAndSetExpectations("sample-index", "type", "2", "{\"name\":\"Mike\"}");
        records.add(r2);
        ElasticsearchObject r3 = createMockRecordAndSetExpectations("sample-index", "type", "3", "{\"name\":\"Mike\"}");
        records.add(r3);

        // mock building and executing the request
        mockBuildingAndExecutingRequest(records);

        // Verify that there is a response for each record, and that each gets touched.
        BulkItemResponse[] responses = new BulkItemResponse[records.size()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = createMock(BulkItemResponse.class);
            expect(responses[i].isFailed()).andReturn(false);
            replay(responses[i]);
        }
        expect(mockBulkResponse.getItems()).andReturn(responses);

        replay(elasticsearchClientMock, r1, r2, r3, buffer, mockBulkBuilder, mockIndexBuilder, mockFuture,
                mockBulkResponse);

        List<ElasticsearchObject> failures = emitter.emit(buffer);
        assertTrue(failures.isEmpty());

        verify(elasticsearchClientMock, r1, r2, r3, buffer, mockBulkBuilder, mockIndexBuilder, mockFuture,
                mockBulkResponse);
        verifyRecords(records);
        verifyResponses(responses);
    }

    /**
     * Set the 2nd record in the passed in list to fail.
     * 
     * Assert that only 1 record is returned, and that it is equal to the 2nd object in the list.
     * 
     * @throws IOException
     */
    @Test
    public void testRecordFails() throws IOException {

        List<ElasticsearchObject> records = new ArrayList<ElasticsearchObject>();
        ElasticsearchObject r1 = createMockRecordAndSetExpectations("sample-index", "type", "1", "{\"name\":\"Mike\"}");
        records.add(r1);
        // this will be the failing record.
        ElasticsearchObject r2 = createMockRecordAndSetExpectations("sample-index", "type", "2",
                "{\"name\":\"Mike\",\"badJson\":\"forgotendingquote}");
        records.add(r2);
        ElasticsearchObject r3 = createMockRecordAndSetExpectations("sample-index", "type", "3", "{\"name\":\"Mike\"}");
        records.add(r3);

        // mock building and executing the request
        mockBuildingAndExecutingRequest(records);

        // Verify that there is a response for each record, and that each gets touched.
        BulkItemResponse[] responses = new BulkItemResponse[records.size()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = createMock(BulkItemResponse.class);
            // define behavior for a failing record
            if (i == 1) {
                expect(responses[i].isFailed()).andReturn(true);
                expect(responses[i].getFailureMessage()).andReturn("bad json error message");
                Failure failure = new Failure("index", "type", "id", "foo failure message", RestStatus.BAD_REQUEST);
                expect(responses[i].getFailure()).andReturn(failure);
            } else {
                expect(responses[i].isFailed()).andReturn(false);
            }
            replay(responses[i]);
        }

        // verify admin client gets used to check cluster status. this case, yellow
        expect(mockBulkResponse.getItems()).andReturn(responses);
        AdminClient mockAdminClient = createMock(AdminClient.class);
        expect(elasticsearchClientMock.admin()).andReturn(mockAdminClient);
        ClusterAdminClient mockClusterAdminClient = createMock(ClusterAdminClient.class);
        expect(mockAdminClient.cluster()).andReturn(mockClusterAdminClient);
        ClusterHealthRequestBuilder mockHealthRequestBuilder = createMock(ClusterHealthRequestBuilder.class);
        expect(mockClusterAdminClient.prepareHealth()).andReturn(mockHealthRequestBuilder);
        ListenableActionFuture mockHealthFuture = createMock(ListenableActionFuture.class);
        expect(mockHealthRequestBuilder.execute()).andReturn(mockHealthFuture);
        ClusterHealthResponse mockResponse = createMock(ClusterHealthResponse.class);
        expect(mockHealthFuture.actionGet()).andReturn(mockResponse);
        expect(mockResponse.getStatus()).andReturn(ClusterHealthStatus.YELLOW);
        expectLastCall().times(2);

        replay(elasticsearchClientMock, r1, r2, r3, buffer, mockBulkBuilder, mockIndexBuilder, mockFuture,
                mockBulkResponse, mockAdminClient, mockClusterAdminClient, mockHealthRequestBuilder, mockHealthFuture,
                mockResponse);

        List<ElasticsearchObject> failures = emitter.emit(buffer);
        assertTrue(failures.size() == 1);
        // the emitter should return the exact object that failed
        assertEquals(failures.get(0), records.get(1));

        verify(elasticsearchClientMock, r1, r2, r3, buffer, mockBulkBuilder, mockIndexBuilder, mockFuture,
                mockBulkResponse, mockAdminClient, mockClusterAdminClient, mockHealthRequestBuilder, mockHealthFuture,
                mockResponse);
        verifyRecords(records);
        verifyResponses(responses);
    }

    /**
     * Check that emitter will retry on NoNodeAvailableException.
     * 
     * This exception would be thrown on the BulkRequest.execute() call
     */
    @Test
    public void testRetiresWhenNoNodesAvailable() throws IOException {
        List<ElasticsearchObject> records = new ArrayList<ElasticsearchObject>();
        ElasticsearchObject r1 = createMockRecordAndSetExpectations("sample-index", "type", "1", "{\"name\":\"Mike\"}");
        records.add(r1);
        ElasticsearchObject r2 = createMockRecordAndSetExpectations("sample-index", "type", "2", "{\"name\":\"Mike\"}");
        records.add(r2);
        ElasticsearchObject r3 = createMockRecordAndSetExpectations("sample-index", "type", "3", "{\"name\":\"Mike\"}");
        records.add(r3);

        // mock building the request
        mockBuildingRequest(records);

        // mock execution, throw NoNodeAvailable
        expect(mockBulkBuilder.execute()).andThrow(new NoNodeAvailableException());
        expectLastCall().times(3);
        // mock execution and and nodes are back
        expect(mockBulkBuilder.execute()).andReturn(mockFuture);
        expect(mockFuture.actionGet()).andReturn(mockBulkResponse);

        // Verify that there is a response for each record, and that each gets touched.
        BulkItemResponse[] responses = new BulkItemResponse[records.size()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = createMock(BulkItemResponse.class);
            expect(responses[i].isFailed()).andReturn(false);
            replay(responses[i]);
        }
        expect(mockBulkResponse.getItems()).andReturn(responses);

        replay(elasticsearchClientMock, r1, r2, r3, buffer, mockBulkBuilder, mockIndexBuilder, mockFuture,
                mockBulkResponse);

        List<ElasticsearchObject> failures = emitter.emit(buffer);
        assertTrue(failures.isEmpty());

        verify(elasticsearchClientMock, r1, r2, r3, buffer, mockBulkBuilder, mockIndexBuilder, mockFuture,
                mockBulkResponse);
        verifyRecords(records);
        verifyResponses(responses);
    }
}
