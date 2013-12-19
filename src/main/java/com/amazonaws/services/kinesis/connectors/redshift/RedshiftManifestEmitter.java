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
package com.amazonaws.services.kinesis.connectors.redshift;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

/**
 * This implementation of IEmitter collects filenames from a Kinesis stream that has been started by
 * a S3ManifestEmitter. The RedshiftManifestEmitter processes the list of S3 file names, generates a
 * manifest file and performs a Redshift copy. The Redshift copy is done using transactions to
 * prevent duplication of objects in Redshift. <br>
 * It follows the following procedure:
 * <ol>
 * <li>Write manifest file to S3</li>
 * <li>Begin Redshift transaction</li>
 * <li>If any files already exist in Redshift, return and checkpoint (this transaction already
 * completed successfully so no need to duplicate)</li>
 * <li>Write file names to Redshift file table</li>
 * <li>Call Redshift copy</li>
 * <li>Commit Redshift Transaction</li>
 * </ol>
 * <p>
 * This class requires the configuration of an S3 bucket and endpoint, as well as the following
 * Redshift items:
 * <ul>
 * <li>Redshift URL</li>
 * <li>username and password</li>
 * <li>data table and key column (data table stores items from the manifest copy)</li>
 * <li>file table and key column (file table is used to store file names to prevent duplicate
 * entries)</li>
 * <li>mandatory flag for Redshift copy</li>
 * <li>the delimiter used for string parsing when inserting entries into Redshift</li>
 * </ul>
 * <br>
 * NOTE: S3 bucket and Redshift table must be in the same region for Manifest Copy.
 */
public class RedshiftManifestEmitter implements IEmitter<String> {
    private static final Log LOG = LogFactory.getLog(RedshiftManifestEmitter.class);
    private final String s3Bucket;
    private final String dataTable;
    private final String fileTable;
    private final String fileKeyColumn;
    private final char dataDelimiter;
    private final String accessKey;
    private final String secretKey;
    private final String s3Endpoint;
    private final AmazonS3Client s3Client;
    private final boolean copyMandatory;
    private final Properties loginProps;
    private final String redshiftURL;

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
        accessKey = configuration.AWS_CREDENTIALS_PROVIDER.getCredentials().getAWSAccessKeyId();
        secretKey = configuration.AWS_CREDENTIALS_PROVIDER.getCredentials().getAWSSecretKey();
        loginProps = new Properties();
        loginProps.setProperty("user", configuration.REDSHIFT_USERNAME);
        loginProps.setProperty("password", configuration.REDSHIFT_PASSWORD);
        redshiftURL = configuration.REDSHIFT_URL;
    }

    @Override
    public List<String> emit(final UnmodifiableBuffer<String> buffer) throws IOException {
        List<String> records = buffer.getRecords();
        Connection conn = null;
        writeManifestToS3(records);
        try {
            conn = DriverManager.getConnection(redshiftURL, loginProps);
            conn.setAutoCommit(false);
            if (checkForExistingFiles(conn, records)) {
                // All of these files were already written
                conn.rollback();
                // after return, record processor will checkpoint
                closeConnection(conn);
                records.clear();
                return Collections.emptyList();
            }
            insertRecords(conn, records);
            redshiftCopy(conn, records);
            conn.commit();
            LOG.info("Successful Redshift manifest copy of " + getNumberOfCopiedRecords(conn)
                    + " records using manifest s3://" + s3Bucket + "/" + getManifestFile(records));
            closeConnection(conn);
            return Collections.emptyList();
        } catch (SQLException | IOException e) {
            LOG.error(e);
            try {
                conn.rollback();
            } catch (SQLException e1) {
                LOG.error("Could not rollback redshift transaction", e1);
            }
            // All records will be retried
            closeConnection(conn);
            return buffer.getRecords();
        }
    }

    private void closeConnection(Connection conn) {
        try {
            conn.close();
        } catch (Exception e) {

        }
    }

    @Override
    public void fail(List<String> records) {
        for (String record : records) {
            LOG.error("Record failed: " + record);
        }
    }

    /**
     * Generates manifest file and writes it to S3
     * 
     * @param records
     * @throws IOException
     */
    private void writeManifestToS3(List<String> records) throws IOException {
        String fileContents = generateManifestFile(records);
        // upload generated manifest file
        PutObjectRequest putObjectRequest = new PutObjectRequest(s3Bucket, getManifestFile(records),
                new ByteArrayInputStream(fileContents.getBytes()), null);
        s3Client.putObject(putObjectRequest);
    }

    /**
     * 
     * Inserts the records to the fileTable using a SQL String in the format: INSERT INTO fileTable
     * VALUES ('f1'),('f2'),...;
     * 
     * @param records
     * @throws IOException
     */
    private void insertRecords(Connection conn, List<String> records) throws IOException {
        String toInsert = getSet(records, "(", "),(", ")");
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO ");
        insertSQL.append(fileTable);
        insertSQL.append(" VALUES ");
        insertSQL.append(toInsert);
        insertSQL.append(";");
        executeStatement(conn, insertSQL.toString());
    }

    /**
     * Selects the count of files that are already present in Redshift using a SQL Query in the
     * format: SELECT COUNT(*) FROM fileTable WHERE fileKeyColumn IN ('f1','f2',...);
     * 
     * @param records
     * @return true if some files are already present in Redshift, false otherwise
     * @throws IOException
     */

    private boolean checkForExistingFiles(Connection conn, List<String> records) throws IOException {
        String files = getSet(records, "(", ",", ")");
        StringBuilder selectExistingCount = new StringBuilder();
        selectExistingCount.append("SELECT COUNT(*) FROM ");
        selectExistingCount.append(fileTable);
        selectExistingCount.append(" WHERE ");
        selectExistingCount.append(fileKeyColumn);
        selectExistingCount.append(" IN ");
        selectExistingCount.append(files);
        selectExistingCount.append(";");
        Statement stmt = null;
        ResultSet resultSet = null;
        boolean foundDuplicate = false;
        try {
            stmt = conn.createStatement();
            final String query = selectExistingCount.toString();
            resultSet = stmt.executeQuery(query);
            resultSet.next();
            int numDuplicates = resultSet.getInt(1);
            resultSet.close();
            stmt.close();
            if (numDuplicates == records.size()) {
                foundDuplicate = true;
            } else if (numDuplicates != 0) {
                closeConnection(conn);
                throw new IllegalStateException("partial insert detected");
            }
            return foundDuplicate;
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
     * Executes a Redshift copy from S3 using a Manifest file with a command in the format: COPY
     * dataTable FROM 's3://s3Bucket/manifestFile' CREDENTIALS
     * 'aws_access_key_id=accessKey;aws_secret_access_key=secretKey' DELIMITER dataDelimiter
     * MANIFEST;
     * 
     * @param records
     * @throws IOException
     */
    private void redshiftCopy(Connection conn, List<String> records) throws IOException {
        String manifestFile = getManifestFile(records);
        StringBuilder redshiftCopy = new StringBuilder();
        redshiftCopy.append("COPY " + dataTable + " ");
        redshiftCopy.append("FROM 's3://" + s3Bucket + "/" + manifestFile + "' ");
        redshiftCopy.append("CREDENTIALS '");
        redshiftCopy.append("aws_access_key_id=" + accessKey);
        redshiftCopy.append(";");
        redshiftCopy.append("aws_secret_access_key=" + secretKey);
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
            LOG.error("s3 endpoint set to: " + s3Endpoint);
            LOG.error("Error executing statement: " + statement, e);
            throw new IOException(e);
        }
    }

    /**
     * Builds a String from the members of a List of String
     * 
     * @param members
     *            List of String, each member will be surrounded by single quotes
     * @param prepend
     *            beginning of String
     * @param delimiter
     *            between each member
     * @param append
     *            end of String
     * @return String in format: {prepend}
     *         '{member1}'{delimiter}'{member2}'{delimiter}...'{lastMember}'{app e n d }
     */
    private String getSet(List<String> members, String prepend, String delimiter, String append) {
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
     * Manifest file is named in the format {firstFileName}-{lastFileName}
     * 
     * @param records
     * @return Manifest file name
     */
    private String getManifestFile(List<String> records) {
        return records.get(0) + "-" + records.get(records.size() - 1);
    }

    /**
     * Format for Redshift Manifest File:
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
     * @return String representation of S3 manifest file
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
