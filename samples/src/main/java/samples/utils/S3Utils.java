/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package samples.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.Region;

public class S3Utils {

    private static Log LOG = LogFactory.getLog(S3Utils.class);

    /**
     * Create an Amazon S3 bucket if it does not exist.
     * 
     * @param client
     *        The {@link AmazonS3Client} with read and write permissions
     * @param bucketName
     *        The bucket to create
     * @throws IllegalStateException
     *         The bucket is not created before timeout occurs
     */
    public static void createBucket(AmazonS3Client client, String bucketName) {
        if (!bucketExists(client, bucketName)) {
            CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName);
            createBucketRequest.setRegion(Region.US_Standard.toString());
            client.createBucket(createBucketRequest);
        }
        long startTime = System.currentTimeMillis();
        long endTime = startTime + 60 * 1000;
        while (!bucketExists(client, bucketName) && endTime > System.currentTimeMillis()) {
            try {
                LOG.info("Waiting for Amazon S3 to create bucket " + bucketName);
                Thread.sleep(1000 * 10);
            } catch (InterruptedException e) {
            }
        }
        if (!bucketExists(client, bucketName)) {
            throw new IllegalStateException("Could not create bucket " + bucketName);
        }
        LOG.info("Created Amazon S3 bucket " + bucketName);
    }

    /**
     * 
     * @param client
     *        The {@link AmazonS3Client} with read permissions
     * @param bucketName
     *        Check if this bucket exists
     * @return true if the Amazon S3 bucket exists, otherwise return false
     */
    private static boolean bucketExists(AmazonS3Client client, String bucketName) {
        return client.doesBucketExist(bucketName);
    }

    /**
     * Deletes an Amazon S3 bucket if it exists.
     * 
     * @param client The {@link AmazonS3Client} with read and write permissions
     * @param bucketName The Amazon S3 bucket to delete
     */
    public static void deleteBucket(AmazonS3Client client, String bucketName) {
        if (bucketExists(client, bucketName)) {
            DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketName);
            client.deleteBucket(deleteBucketRequest);
            LOG.info("Deleted bucket " + bucketName);
        } else {
            LOG.warn("Bucket " + bucketName + " does not exist");
        }
    }
}
