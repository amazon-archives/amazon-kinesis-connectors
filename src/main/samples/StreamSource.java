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
package samples;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import samples.utils.KinesisUtils;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class is a data source for supplying input to the Kinesis stream. It reads lines from the
 * input file specified in the constructor and emits them by calling String.getBytes() into the
 * stream defined in the KinesisConnectorConfiguration.
 */
public class StreamSource implements Runnable {
    private static Log LOG = LogFactory.getLog(StreamSource.class);
    private AmazonKinesisClient kinesisClient;
    private KinesisConnectorConfiguration config;
    private BufferedReader br;
    private final String inputFile;

    /**
     * Creates a new StreamSource
     * 
     * @param config
     *            Configuration to determine which stream to put records to and get
     *            {@link AWSCredentialsProvider}
     * @param inputFile
     *            File containing record data to emit on each line
     */
    public StreamSource(KinesisConnectorConfiguration config, String inputFile) {
        this.config = config;
        this.inputFile = inputFile;
        kinesisClient = new AmazonKinesisClient(config.AWS_CREDENTIALS_PROVIDER);
        kinesisClient.setEndpoint(config.KINESIS_ENDPOINT);
        KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesisClient, config.KINESIS_INPUT_STREAM, 2);
    }

    @Override
    public void run() {
        InputStream inputStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(inputFile);
        if (inputStream == null) {
            throw new IllegalStateException("Could not find input file: " + inputFile);
        }
        br = new BufferedReader(new InputStreamReader(inputStream));
        String user;
        try {
            while ((user = br.readLine()) != null) {
                KinesisMessageModel map = new ObjectMapper().readValue(user, KinesisMessageModel.class);
                PutRecordRequest putRecordRequest = new PutRecordRequest();
                putRecordRequest.setStreamName(config.KINESIS_INPUT_STREAM);
                ByteBuffer data = ByteBuffer.wrap(user.getBytes());
                putRecordRequest.setData(data);
                putRecordRequest.setPartitionKey(map.userid + "");
                kinesisClient.putRecord(putRecordRequest);
            }
            br.close();
        } catch (Exception e) {
            LOG.error(e);
            try {
                br.close();
            } catch (Exception e1) {
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        br.close();
    }
}
