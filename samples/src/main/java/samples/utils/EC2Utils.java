/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
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
