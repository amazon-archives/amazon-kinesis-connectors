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
package com.amazonaws.services.kinesis.connectors.redshift;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

/**
 * This implementation of IEmitter collects filenames from an Amazon Kinesis stream that has been started by
 * a S3ManifestEmitter. The RedshiftManifestEmitter processes the list of Amazon S3 file names, generates a
 * manifest file and performs an Amazon Redshift copy. The Amazon Redshift copy is done using transactions to
 * prevent duplication of objects in Amazon Redshift. <br>
 * It follows the following procedure:
 * <ol>
 * <li>Write manifest file to Amazon S3</li>
 * <li>Begin Amazon Redshift transaction</li>
 * <li>If any files already exist in Amazon Redshift, return and checkpoint (this transaction already completed
 * successfully so no need to duplicate)</li>
 * <li>Write file names to Amazon Redshift file table</li>
 * <li>Call Amazon Redshift copy</li>
 * <li>Commit Amazon Redshift Transaction</li>
 * </ol>
 * <p>
 * This class requires the configuration of an Amazon S3 bucket and endpoint, as well as the following Amazon Redshift
 * items:
 * <ul>
 * <li>Amazon Redshift URL</li>
 * <li>username and password</li>
 * <li>data table and key column (data table stores items from the manifest copy)</li>
 * <li>file table and key column (file table is used to store file names to prevent duplicate entries)</li>
 * <li>mandatory flag for Amazon Redshift copy</li>
 * <li>the delimiter used for string parsing when inserting entries into Amazon Redshift</li>
 * </ul>
 * <br>
 * NOTE: Amazon S3 bucket and Amazon Redshift table must be in the same region for Manifest Copy.
 */
public class RedshiftManifestEmitter implements IEmitter<String> {
    private static final Log LOG = LogFactory.getLog(RedshiftManifestEmitter.class);
    private final String s3Bucket;
    private final String dataTable;
    private final String fileTable;
    private final String fileKeyColumn;
    private final char dataDelimiter;
    private final AWSCredentialsProvider credentialsProvider;
    private final String s3Endpoint;
    private final AmazonS3Client s3Client;
    private final boolean copyMandatory;
    private final Properties loginProps;
    private final String redshiftURL;
    private static final String MANIFEST_PREFIX = "manifests/";

    public RedshiftManifestEmitter(KinesisConnectorConfiguration configuration) {
        dataTable = configuration.REDSHIFT_DATA_TABLE;
        fileTable = configuration.REDSHIFT_FILE_TABLE;
        fileKeyColumn = configuration.REDSHIFT_FILE_KEY_COLUMN;
        dataDelimiter = configuration.REDSHIFT_DATA_DELIMITER;
        copyMandatory = configuration.REDSHIFT_COPY_MANDATORY;
        s3Bucket = configuration.S3_BUCKET;
        s3Endpoint = configuration.S3_ENDPOINT;
        s3Client = new AmazonS3Client(configuration.AWS_CREDENTIALS_PROVIDER);
        if (s3Endpoint != null) {
            s3Client.setEndpoint(s3Endpoint);
        }
        credentialsProvider = configuration.AWS_CREDENTIALS_PROVIDER;
        loginProps = new Properties();
        loginProps.setProperty("user", configuration.REDSHIFT_USERNAME);
        loginProps.setProperty("password", configuration.REDSHIFT_PASSWORD);
        redshiftURL = configuration.REDSHIFT_URL;
    }

    @Override
    public List<String> emit(final UnmodifiableBuffer<String> buffer) throws IOException {
        List<String> records = buffer.getRecords();
        Connection conn = null;

        String manifestFileName = getManifestFile(records);
        // Copy to Amazon Redshift using manifest file
        try {
            conn = DriverManager.getConnection(redshiftURL, loginProps);
            conn.setAutoCommit(false);
            List<String> deduplicatedRecords = checkForExistingFiles(conn, records);
            if (deduplicatedRecords.isEmpty()) {
                LOG.info("All the files in this set were already copied to Redshift.");
                // All of these files were already written
                rollbackAndCloseConnection(conn);
                return Collections.emptyList();
            }

            if (deduplicatedRecords.size() != records.size()) {
                manifestFileName = getManifestFile(deduplicatedRecords);
            }
            // Write manifest file to Amazon S3
            try {
                writeManifestToS3(manifestFileName, deduplicatedRecords);
            } catch (Exception e) {
                LOG.error("Error writing file " + manifestFileName + " to S3. Failing this emit attempt.", e);
                rollbackAndCloseConnection(conn);
                return buffer.getRecords();
            }

            LOG.info("Inserting " + deduplicatedRecords.size() + " rows into the files table.");
            insertRecords(conn, deduplicatedRecords);
            LOG.info("Initiating Amazon Redshift manifest copy of " + deduplicatedRecords.size() + " files.");
            redshiftCopy(conn, manifestFileName);
            conn.commit();
            LOG.info("Successful Amazon Redshift manifest copy of " + getNumberOfCopiedRecords(conn) + " records from "
                    + deduplicatedRecords.size() + " files using manifest s3://" + s3Bucket + "/"
                    + getManifestFile(records));
            closeConnection(conn);
            return Collections.emptyList();
        } catch (SQLException | IOException e) {
            LOG.error("Error copying data from manifest file " + manifestFileName
                    + " into Amazon Redshift. Failing this emit attempt.", e);
            rollbackAndCloseConnection(conn);
            return buffer.getRecords();
        } catch (Exception e) {
            LOG.error("Error copying data from manifest file " + manifestFileName
                    + " into Redshift. Failing this emit attempt.", e);
            rollbackAndCloseConnection(conn);
            return buffer.getRecords();
        }
    }

    private void rollbackAndCloseConnection(Connection conn) {
        try {
            if ((conn != null) && (!conn.isClosed())) {
                conn.rollback();
            }
        } catch (Exception e) {
            LOG.error("Unable to rollback Amazon Redshift transaction.", e);
        }
        closeConnection(conn);
    }

    private void closeConnection(Connection conn) {
        try {
            if ((conn != null) && (!conn.isClosed())) {
                conn.close();
            }
        } catch (Exception e) {
            LOG.error("Unable to close Amazon Redshift connection.", e);
        }
    }

    @Override
    public void fail(List<String> records) {
        for (String record : records) {
            LOG.error("Record failed: " + record);
        }
    }

    /**
     * Generates manifest file and writes it to Amazon S3
     * 
     * @param fileName Name of manifest file (Amazon S3 key)
     * @param records Used to generate the manifest file
     * @throws IOException
     */
    private String writeManifestToS3(String fileName, List<String> records) throws IOException {
        String fileContents = generateManifestFile(records);
        // upload generated manifest file
        PutObjectRequest putObjectRequest =
                new PutObjectRequest(s3Bucket, fileName, new ByteArrayInputStream(fileContents.getBytes()), null);
        s3Client.putObject(putObjectRequest);
        return fileName;
    }

    /**
     * 
     * Inserts the records to the fileTable using a SQL String in the format: INSERT INTO fileTable
     * VALUES ('f1'),('f2'),...;
     * 
     * @param records
     * @throws IOException
     */
    private void insertRecords(Connection conn, Collection<String> records) throws IOException {
        String toInsert = getCollectionString(records, "(", "),(", ")");
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO ");
        insertSQL.append(fileTable);
        insertSQL.append(" VALUES ");
        insertSQL.append(toInsert);
        insertSQL.append(";");
        executeStatement(conn, insertSQL.toString());
    }

    /**
     * Selects the count of files that are already present in Amazon Redshift using a SQL Query in the
     * format: SELECT COUNT(*) FROM fileTable WHERE fileKeyColumn IN ('f1','f2',...);
     * 
     * @param records
     * @return Deduplicated list of files
     * @throws IOException
     */

    private List<String> checkForExistingFiles(Connection conn, List<String> records) throws IOException {
        SortedSet<String> recordSet = new TreeSet<>(records);
        String files = getCollectionString(recordSet, "(", ",", ")");
        StringBuilder selectExisting = new StringBuilder();
        selectExisting.append("SELECT " + fileKeyColumn + " FROM ");
        selectExisting.append(fileTable);
        selectExisting.append(" WHERE ");
        selectExisting.append(fileKeyColumn);
        selectExisting.append(" IN ");
        selectExisting.append(files);
        selectExisting.append(";");
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            stmt = conn.createStatement();
            final String query = selectExisting.toString();
            resultSet = stmt.executeQuery(query);
            while (resultSet.next()) {
                String existingFile = resultSet.getString(1);
                LOG.info("File " + existingFile + " has already been copied. Leaving it out.");
                recordSet.remove(existingFile);
            }
            resultSet.close();
            stmt.close();
            return new ArrayList<String>(recordSet);
        } catch (SQLException e) {
            try {
                resultSet.close();
            } catch (Exception e1) {
            }
            try {
                stmt.close();
            } catch (Exception e1) {
            }
            throw new IOException(e);
        }
    }

    private int getNumberOfCopiedRecords(Connection conn) throws IOException {
        String cmd = "select pg_last_copy_count();";
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(cmd);
            resultSet.next();
            int numCopiedRecords = resultSet.getInt(1);
            resultSet.close();
            stmt.close();
            return numCopiedRecords;
        } catch (SQLException e) {
            try {
                resultSet.close();
            } catch (Exception e1) {
            }
            try {
                stmt.close();
            } catch (Exception e1) {
            }
            throw new IOException(e);
        }

    }

    /**
     * Executes a, Amazon Redshift copy from Amazon S3 using a Manifest file with a command in the format: COPY
     * dataTable FROM 's3://s3Bucket/manifestFile' CREDENTIALS
     * 'aws_access_key_id=accessKey;aws_secret_access_key=secretKey' DELIMITER dataDelimiter
     * MANIFEST;
     * 
     * @param Name of manifest file
     * @throws IOException
     */
    protected void redshiftCopy(Connection conn, String manifestFile) throws IOException {
        AWSCredentials credentials = credentialsProvider.getCredentials();
        StringBuilder redshiftCopy = new StringBuilder();
        redshiftCopy.append("COPY " + dataTable + " ");
        redshiftCopy.append("FROM 's3://" + s3Bucket + "/" + manifestFile + "' ");
        redshiftCopy.append("CREDENTIALS '");
        redshiftCopy.append("aws_access_key_id=" + credentials.getAWSAccessKeyId());
        redshiftCopy.append(";");
        redshiftCopy.append("aws_secret_access_key=" + credentials.getAWSSecretKey());
        if (credentials instanceof AWSSessionCredentials) {
            redshiftCopy.append(";");
            redshiftCopy.append("token=" + ((AWSSessionCredentials) credentials).getSessionToken());
        }
        redshiftCopy.append("' ");
        redshiftCopy.append("DELIMITER '" + dataDelimiter + "' ");
        redshiftCopy.append("MANIFEST");
        redshiftCopy.append(";");
        executeStatement(conn, redshiftCopy.toString());
    }

    /**
     * Helper function to execute SQL Statement with no results. Attempts to execute the statement
     * redshiftRetryLimit times.
     * 
     * @param statement
     * @throws IOException
     */
    private void executeStatement(Connection conn, String statement) throws IOException {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(statement);
            stmt.close();
            return;
        } catch (SQLException e) {
            LOG.error("Amazon S3 endpoint set to: " + s3Endpoint);
            LOG.error("Error executing statement: " + statement, e);
            throw new IOException(e);
        }
    }

    /**
     * Builds a String from the members of a Set of String
     * 
     * @param members
     *        List of String, each member will be surrounded by single quotes
     * @param prepend
     *        beginning of String
     * @param delimiter
     *        between each member
     * @param append
     *        end of String
     * @return String in format: {prepend}
     *         '{member1}'{delimiter}'{member2}'{delimiter}...'{lastMember}'{app e n d }
     */
    private String getCollectionString(Collection<String> members, String prepend, String delimiter, String append) {
        StringBuilder s = new StringBuilder();
        s.append(prepend);
        for (String m : members) {
            s.append("'");
            s.append(m);
            s.append("'");
            s.append(delimiter);
        }
        s.replace(s.length() - delimiter.length(), s.length(), "");
        s.append(append);
        return s.toString();
    }

    /**
     * Manifest file is named in the format manifests/{firstFileName}-{lastFileName}
     * 
     * @param records
     * @return Manifest file name
     */
    private String getManifestFile(List<String> records) {
        return MANIFEST_PREFIX + records.get(0) + "-" + records.get(records.size() - 1);
    }

    /**
     * Format for Amazon Redshift Manifest File:
     * 
     * <pre>
     * {
     * 	"entries": [
     * 		{"url":"s3://s3Bucket/file1","mandatory":true},
     * 		{"url":"s3://s3Bucket/file2","mandatory":true},
     * 		{"url":"s3://s3Bucket/file3","mandatory":true}
     * 	]
     * }
     * 
     * </pre>
     * 
     * 
     * @param files
     * @return String representation of Amazon S3 manifest file
     */
    private String generateManifestFile(List<String> files) {
        StringBuilder s = new StringBuilder();
        s.append("{\n");
        s.append("\t\"entries\": [\n");
        for (String file : files) {
            s.append("\t\t{");
            s.append("\"url\":\"s3://");
            s.append(s3Bucket);
            s.append("/");
            s.append(file);
            s.append("\"");
            s.append(",");
            s.append("\"mandatory\":" + Boolean.toString(copyMandatory));
            s.append("},\n");
        }
        s.replace(s.length() - 2, s.length() - 1, "");
        s.append("\t]\n");
        s.append("}\n");
        return s.toString();
    }

    @Override
    public void shutdown() {
        s3Client.shutdown();
    }

}
