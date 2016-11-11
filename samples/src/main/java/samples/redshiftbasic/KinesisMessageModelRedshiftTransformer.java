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
package samples.redshiftbasic;

import samples.KinesisMessageModel;
import samples.redshiftmanifest.S3ManifestExecutor;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.redshift.RedshiftTransformer;

/**
 * Custom transformer used by the {@link S3ManifestExecutor} and {@link RedshiftBasicExecutor} to convert
 * {@link KinesisMessageModel} records to delimited Strings used by Amazon Redshift copy.
 */
public class KinesisMessageModelRedshiftTransformer extends RedshiftTransformer<KinesisMessageModel> {
    private final char delim;

    /**
     * Creates a new KinesisMessageModelRedshiftTransformer.
     * 
     * @param config The configuration containing the Amazon Redshift data delimiter
     */
    public KinesisMessageModelRedshiftTransformer(KinesisConnectorConfiguration config) {
        super(KinesisMessageModel.class);
        delim = config.REDSHIFT_DATA_DELIMITER;
    }

    @Override
    public String toDelimitedString(KinesisMessageModel record) {
        StringBuilder b = new StringBuilder();
        b.append(record.userid)
                .append(delim)
                .append(record.username)
                .append(delim)
                .append(record.firstname)
                .append(delim)
                .append(record.lastname)
                .append(delim)
                .append(record.city)
                .append(delim)
                .append(record.state)
                .append(delim)
                .append(record.email)
                .append(delim)
                .append(record.phone)
                .append(delim)
                .append(record.likesports)
                .append(delim)
                .append(record.liketheatre)
                .append(delim)
                .append(record.likeconcerts)
                .append(delim)
                .append(record.likejazz)
                .append(delim)
                .append(record.likeclassical)
                .append(delim)
                .append(record.likeopera)
                .append(delim)
                .append(record.likerock)
                .append(delim)
                .append(record.likevegas)
                .append(delim)
                .append(record.likebroadway)
                .append(delim)
                .append(record.likemusicals)
                .append("\n");

        return b.toString();
    }

}
