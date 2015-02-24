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


import static com.strategicgains.docussandra.bucketmanagement.ConversionUtils.bytes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;




/**
 * Simple implementation that does static hashing across 100 rows. Future implementations should be smarter and create
 * new tokens as required when buckets become too large for an entity property within an application for the given index
 * type.
 *
 * @author tnine
 */
public class SimpleIndexBucketLocatorImpl implements IndexBucketLocator {

    public static final Long MINIMUM = 0l;
    public static final Long MAXIMUM = Long.MAX_VALUE;

    private final List<Long> buckets = new ArrayList<>( 100 );
    private final List<String> bucketsString = new ArrayList<>( 100 );
    private final int size;


    /** Create a bucket locator with the specified size */
    public SimpleIndexBucketLocatorImpl( int size ) {
        for ( int i = 0; i < size; i++ ) {
            Long integer = initialToken( size, i );
            buckets.add( integer );
            bucketsString.add( String.format( "%039d", integer ) );
        }

        this.size = size;
    }


    /** Base constructor that creates a ring of 100 tokens */
    public SimpleIndexBucketLocatorImpl() {
        this( 100 );
    }


    /** Get a token */
    private static Long initialToken( int size, int position ) {
        Long decValue = MINIMUM;
        if ( position != 0 ) {
            decValue = ((MAXIMUM / size) * position) - 1;
        }
        return decValue;
    }


    /** Get the next token in the ring for this big int. */
    private String getClosestToken( UUID entityId ) { //TODO: consider scrapping UUIDs and just going with longs
        byte[] bytes = bytes( entityId );
        byte[] finalBytes = new byte[8];
        if(bytes.length > 8){
            for(int i = 0; i < finalBytes.length; i++){
                finalBytes[i] = bytes[i];
            }
        } else {
            finalBytes = bytes;//i doubt this will ever happen
        }
        ByteBuffer bb = ByteBuffer.wrap(finalBytes);
        Long location = bb.getLong();
        location = Math.abs(location);

        int index = Collections.binarySearch( buckets, location );

        if ( index < 0 ) {
            index = ( index + 1 ) * -1;
        }

        // mod if we need to wrap
        index = index % size;

        return bucketsString.get( index );
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.usergrid.persistence.IndexBucketLocator#getBucket(java.util.UUID,
     * org.apache.usergrid.persistence.IndexBucketLocator.IndexType, java.util.UUID,
     * java.lang.String[])
     */
    @Override
    public String getBucket( UUID applicationId, UUID entityId, String... components ) {
        return getClosestToken( entityId );
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.usergrid.persistence.IndexBucketLocator#getBuckets(java.util.UUID,
     * org.apache.usergrid.persistence.IndexBucketLocator.IndexType,
     * java.lang.String[])
     */
    @Override
    public List<String> getBuckets( UUID applicationId, String... components ) {
        return bucketsString;
    }
}
