/*
 * Copyright 2015 udeyoje.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static com.strategicgains.docussandra.bucketmanagement.ConversionUtils.bytebuffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.thrift.IndexType;
import org.apache.commons.lang.StringUtils;
import static org.apache.commons.lang.StringUtils.split;
import static org.apache.commons.lang.StringUtils.substringAfterLast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author udeyoje
 */
public class NewCassandraPersistanceUtils {

    public static final String PROPERTY_TYPE = "type";
    public static final String PROPERTY_UUID = "uuid";

    private static final Logger logger = LoggerFactory.getLogger(NewCassandraPersistanceUtils.class);

    /**
     *
     */
    public static final ByteBuffer PROPERTY_TYPE_AS_BYTES = ConversionUtils.bytebuffer(PROPERTY_TYPE);

    /**
     *
     */
    public static final ByteBuffer PROPERTY_ID_AS_BYTES = ConversionUtils.bytebuffer(PROPERTY_UUID);

    /**
     *
     */
    public static final char KEY_DELIM = ':';

    /**
     *
     */
    public static final UUID NULL_ID = new UUID(0, 0);

    /**
     * @return a composite key
     */
    public static Object key(Object... objects) {
        if (objects.length == 1) {
            Object obj = objects[0];
            if ((obj instanceof UUID) || (obj instanceof ByteBuffer)) {
                return obj;
            }
        }
        StringBuilder s = new StringBuilder();
        for (Object obj : objects) {
            if (obj instanceof String) {
                s.append(((String) obj).toLowerCase());
            } else if (obj instanceof List<?>) {
                s.append(key(((List<?>) obj).toArray()));
            } else if (obj instanceof Object[]) {
                s.append(key((Object[]) obj));
            } else if (obj != null) {
                s.append(obj);
            } else {
                s.append("*");
            }

            s.append(KEY_DELIM);
        }

        s.deleteCharAt(s.length() - 1);

        return s.toString();
    }

    public static List<ColumnDefinition> getIndexMetadata(String indexes) {
        if (indexes == null) {
            return null;
        }
        String[] index_entries = split(indexes, ',');
        List<org.apache.cassandra.thrift.ColumnDef> columns = new ArrayList<org.apache.cassandra.thrift.ColumnDef>();
        for (String index_entry : index_entries) {
            String column_name = stringOrSubstringBeforeFirst(index_entry, ':').trim();
            String comparer = substringAfterLast(index_entry, ":").trim();
            if (StringUtils.isBlank(comparer)) {
                comparer = "UUIDType";
            }
            if (StringUtils.isNotBlank(column_name)) {
                org.apache.cassandra.thrift.ColumnDef cd = new org.apache.cassandra.thrift.ColumnDef(bytebuffer(column_name), comparer);
                cd.setIndex_name(column_name);
                cd.setIndex_type(IndexType.KEYS);
                columns.add(cd);
            }
        }
        return null; //TODO: udeyoje: finish ThriftColumnDef.fromThriftList(columns);
    }

    public static String stringOrSubstringBeforeFirst(String str, char c) {
        if (str == null) {
            return null;
        }
        int i = str.indexOf(c);
        if (i != -1) {
            return str.substring(0, i);
        }
        return str;
    }

}
