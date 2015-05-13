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
package com.strategicgains.docussandra.cache;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Semaphore used to control access to both the objects that are cached and the
 * caches themselves.
 *
 * @author udeyoje
 */
public class CacheSynchronizer
{

    private static ConcurrentHashMap<Key, Object> semaphoreMap = new ConcurrentHashMap<>();

    //TODO: purge/cleanup map at some point
    public static synchronized Object getLockingObject(String stringKey, Class clazz)
    {
        Key key = new Key(stringKey, clazz.getCanonicalName());
        Object lock = semaphoreMap.get(key);
        if (lock == null)
        {
            lock = new Object();
            semaphoreMap.put(key, lock);
        }
        return lock;
    }

    public static synchronized Object getLockingObject(String stringKey, String type)
    {
        Key key = new Key(stringKey, type);
        Object lock = semaphoreMap.get(key);
        if (lock == null)
        {
            lock = new Object();
            semaphoreMap.put(key, lock);
        }
        return lock;
    }

    private static class Key
    {

        private final String keyString;
        private final String type;

        public Key(String keyString, String type)
        {
            this.keyString = keyString;
            this.type = type;
        }

        public String getKeyString()
        {
            return keyString;
        }

        public String getType()
        {
            return type;
        }

        @Override
        public int hashCode()
        {
            int hash = 5;
            hash = 29 * hash + Objects.hashCode(this.keyString);
            hash = 29 * hash + Objects.hashCode(this.type);
            return hash;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            final Key other = (Key) obj;
            if (!Objects.equals(this.keyString, other.keyString))
            {
                return false;
            }
            if (!Objects.equals(this.type, other.type))
            {
                return false;
            }
            return true;
        }

    }
}
