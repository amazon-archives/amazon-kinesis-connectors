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
package samples.redshiftmanifest;

import samples.KinesisConnectorExecutor;
import samples.KinesisMessageModel;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;

/**
 * The executor used to read Amazon S3 file names from the secondary Amazon Kinesis stream, create a manifest file
 * in Amazon S3, and then perform a Amazon Redshift manifest copy.
 */
public class RedshiftManifestExecutor extends KinesisConnectorExecutor<String, String> {

    /**
     * Creates a new RedshiftManifestExecutor.
     * 
     * @param configFile
     *        The name of the configuration file to look for on the classpath.
     */
    public RedshiftManifestExecutor(String configFile) {
        super(configFile);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<String, String> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<>(new RedshiftManifestPipeline(), config);
    }

    private static String REDSHIFT_CONFIG_FILE = "RedshiftManifestSample.properties";
    private static String S3_CONFIG_FILE = "S3ManifestSample.properties";

    /**
     * Main method to run the Amazon Redshift manifest copy sample. Runs both the {@link S3ManifestExecutor} and
     * {@link RedshiftManifestExecutor}
     * 
     * @param args
     */
    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not load PostgreSQL driver");
        }

        if (args.length == 2) {
            REDSHIFT_CONFIG_FILE = args[0];
            S3_CONFIG_FILE = args[1];
        } else if (args.length != 0) {
            System.out.println("Usage: " + RedshiftManifestExecutor.class.getName()
                    + " <RedshiftManifest properties file> <S3Manifest properties file>");
            System.exit(1);
        }

        // Spawn the S3Executor
        KinesisConnectorExecutor<KinesisMessageModel, byte[]> s3ManifestExecutor =
                new S3ManifestExecutor(S3_CONFIG_FILE);
        Thread s3Thread = new Thread(s3ManifestExecutor);
        s3Thread.start();

        // Run the RedshiftExecutor
        KinesisConnectorExecutor<String, String> redshiftExecutor = new RedshiftManifestExecutor(REDSHIFT_CONFIG_FILE);
        redshiftExecutor.run();
    }
}
