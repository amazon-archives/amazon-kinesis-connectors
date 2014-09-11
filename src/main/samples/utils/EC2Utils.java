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

import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.Reservation;

public class EC2Utils {

    /**
     * Return the DNS name of one Amazon EC2 instance with the provided filter name and value.
     * 
     * @param ec2Client
     *        an Amazon EC2 instance
     * @param filterName
     *        the name of the filter
     * @param filterValue
     *        the value of the filter
     * @return the public DNS name of an instance with the filter name and value. Null if none exist.
     */
    public static String getEndpointForFirstActiveInstanceWithTag(AmazonEC2 ec2Client,
            String filterName,
            String filterValue) {
        DescribeInstancesRequest describeInstancesRequest =
                new DescribeInstancesRequest().withFilters(new Filter().withName(filterName).withValues(filterValue));
        DescribeInstancesResult describeInstancesResult = ec2Client.describeInstances(describeInstancesRequest);

        List<Reservation> reservations = describeInstancesResult.getReservations();
        for (Reservation reservation : reservations) {
            List<Instance> ec2Instances = reservation.getInstances();
            for (Instance ec2Instance : ec2Instances) {
                if (InstanceStateName.Running.toString().equals(ec2Instance.getState().getName())) {
                    return ec2Instance.getPublicDnsName();
                }
            }
        }
        return null;
    }
}
