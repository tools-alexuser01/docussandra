/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.strategicgains.docussandra.bucketmanagement;

import com.strategicgains.docussandra.Utils;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;

/**
 * @author tnine
 */
public class SimpleIndexBucketLocatorImplTest
{

    @Test
    public void oneBucket()
    {

        UUID appId = UUIDUtils.newTimeUUID();
        String entityType = "user";
        String propName = "firstName";

        SimpleIndexBucketLocatorImpl locator = new SimpleIndexBucketLocatorImpl(1);

        List<String> buckets = locator.getBuckets(appId, entityType, propName);

        assertEquals(1, buckets.size());

        UUID testId1 = UUIDUtils.minTimeUUID(0l);

        UUID testId2 = UUIDUtils.minTimeUUID(Long.MAX_VALUE / 2);

        UUID testId3 = UUIDUtils.minTimeUUID(Long.MAX_VALUE);

        String bucket1 = locator.getBucket(appId, testId1, entityType, propName);

        String bucket2 = locator.getBucket(appId, testId2, entityType, propName);

        String bucket3 = locator.getBucket(appId, testId3, entityType, propName);

        assertEquals(bucket1, "000000000000000000000000000000000000000");
        assertEquals(bucket2, "000000000000000000000000000000000000000");
        assertEquals(bucket3, "000000000000000000000000000000000000000");
    }

//    @Test
//    @Ignore
//    public void twoBuckets()
//    {
//
//        UUID appId = UUIDUtils.newTimeUUID();
//        String entityType = "user";
//        String propName = "firstName";
//
//        SimpleIndexBucketLocatorImpl locator = new SimpleIndexBucketLocatorImpl(2);
//
//        List<String> buckets = locator.getBuckets(appId, entityType, propName);
//
//        assertEquals(2, buckets.size());
//
//        UUID testId1 = UUIDUtils.minTimeUUID(0l);
//
//        UUID testId2 = UUIDUtils.maxTimeUUID(Long.MAX_VALUE / 2);
//
//        UUID testId3 = UUIDUtils.minTimeUUID(Long.MAX_VALUE);
//
//        String bucket1 = locator.getBucket(appId, testId1, entityType, propName);
//
//        String bucket2 = locator.getBucket(appId, testId2, entityType, propName);
//
//        String bucket3 = locator.getBucket(appId, testId3, entityType, propName);
//
//        String bucket4 = locator.getBucket(appId, UUIDUtils.minTimeUUID(Long.MAX_VALUE - 1), entityType, propName);
//
//        assertEquals(bucket1, "000000000000000000000000000000000000000");
//        assertEquals(bucket2, "085070591730234615865843651857942052863");
//        assertEquals(bucket3, "000000000000000000000000000000000000000");
//        assertEquals(bucket4, "085070591730234615865843651857942052863");
//    }
//    @Test
//    @Ignore
//    public void evenDistribution()
//    {
//
//        UUID appId = UUIDUtils.newTimeUUID();
//        String entityType = "user";
//        String propName = "firstName";
//
//        int bucketSize = 20;
//        float distributionPercentage = .05f;
//
//        // test 100 elements
//        SimpleIndexBucketLocatorImpl locator = new SimpleIndexBucketLocatorImpl(bucketSize);
//
//        List<String> buckets = locator.getBuckets(appId, entityType, propName);
//
//        assertEquals(bucketSize, buckets.size());
//
//        int testSize = 2000000;
//
//        Map<String, Float> counts = new HashMap<String, Float>();
//
//        final Timer hashes
//                = Metrics.newTimer(SimpleIndexBucketLocatorImplTest.class, "responses", TimeUnit.MILLISECONDS,
//                        TimeUnit.SECONDS);
//
//        // ConsoleReporter.enable(1, TimeUnit.SECONDS);
//        /**
//         * Loop through each new UUID and add it's hash to our map
//         */
//        for (int i = 0; i < testSize; i++)
//        {
//            UUID id = UUIDUtils.newTimeUUID();
//
//            final TimerContext context = hashes.time();
//
//            String bucket = locator.getBucket(appId, id, entityType, propName);
//
//            context.stop();
//
//            Float count = counts.get(bucket);
//
//            if (count == null)
//            {
//                count = 0f;
//            }
//
//            counts.put(bucket, ++count);
//        }
//
//        /**
//         * Check each entry is within +- 5% of every subsequent entry
//         */
//        List<String> keys = new ArrayList<String>(counts.keySet());
//        int keySize = keys.size();
//
//        assertEquals(bucketSize, keySize);
//
//        for (int i = 0; i < keySize; i++)
//        {
//
//            float sourceCount = counts.get(keys.get(i));
//
//            for (int j = i + 1; j < keySize; j++)
//            {
//                float destCount = counts.get(keys.get(j));
//
//                // find the maximum allowed value for the assert based on the
//                // largest value in the pair
//                float maxDelta = Math.max(sourceCount, destCount) * distributionPercentage;
//
//                assertEquals(
//                        String.format("Not within %f as percentage for keys '%s' and '%s'", distributionPercentage,
//                                keys.get(i), keys.get(j)), sourceCount, destCount, maxDelta);
//            }
//        }
//    }
    @Test
    public void practicalDistribution()
    {
        SimpleIndexBucketLocatorImpl locator = new SimpleIndexBucketLocatorImpl(200);
        String bucket1 = locator.getBucket(null, Utils.convertStringToFuzzyUUID("adam"));
        String bucket2 = locator.getBucket(null, Utils.convertStringToFuzzyUUID("adam"));
        assertEquals(bucket1, bucket2);
        String bucket3 = locator.getBucket(null, Utils.convertStringToFuzzyUUID("apple"));
        assertEquals(bucket3, bucket2);
        String bucket4 = locator.getBucket(null, Utils.convertStringToFuzzyUUID("zuul"));
        Assert.assertNotEquals(bucket4, bucket3);
        String bucket5 = locator.getBucket(null, Utils.convertStringToFuzzyUUID("zed"));
        assertEquals(bucket4, bucket5);
        String bucket6 = locator.getBucket(null, Utils.convertStringToFuzzyUUID("xray"));
        Assert.assertNotEquals(bucket5, bucket6);
        String bucket7 = locator.getBucket(null, Utils.convertStringToFuzzyUUID("yankee"));
        Assert.assertNotEquals(bucket7, bucket6);
        String bucket8 = locator.getBucket(null, Utils.convertStringToFuzzyUUID("yak"));
        assertEquals(bucket8, bucket7);
    }
}
