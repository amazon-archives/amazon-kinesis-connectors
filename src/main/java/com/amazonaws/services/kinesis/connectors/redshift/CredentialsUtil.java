package com.amazonaws.services.kinesis.connectors.redshift;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;

/**
 * Utility class to handle credentials.
 */
public class CredentialsUtil {

    /**
     * Build a credential argument for Redshift COPY command.
     *
     * @param provider Credential provider.
     * @return credential
     */
    public static String buildCredential(AWSCredentialsProvider provider) {
        AWSCredentials credentials = provider.getCredentials();
        StringBuilder builder = new StringBuilder();
        builder
                .append("aws_access_key_id=")
                .append(credentials.getAWSAccessKeyId())
                .append(";aws_secret_access_key=")
                .append(credentials.getAWSSecretKey());
        if (credentials instanceof AWSSessionCredentials) {
            builder
                    .append(";token=")
                    .append(((AWSSessionCredentials) credentials).getSessionToken());
        }
        return builder.toString();
    }
}
