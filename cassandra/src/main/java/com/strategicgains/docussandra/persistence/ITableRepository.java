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
package com.strategicgains.docussandra.persistence;

import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexIdentifier;

/**
 *
 * @author udeyoje
 */
public interface ITableRepository
{

    /**
     * Creates an iTable for the specified index.
     *
     * @param index Index that needs an iTable created for it.
     */
    void createITable(Index index);

    /**
     * Deletes an iTable
     *
     * @param index index whose iTable should be deleted
     */
    void deleteITable(Index index);

    /**
     * Deletes an iTable
     *
     * @param indexId index id whose iTable should be deleted
     */
    void deleteITable(IndexIdentifier indexId);

    /**
     * Deletes an iTable
     *
     * @param tableName iTable getIndexName to delete.
     */
    void deleteITable(String tableName);

    /**
     * Checks to see if an iTable exists for the specified index.
     *
     * @param index Index that you want to check if it has a corresponding
     * iTable.
     * @return True if the iTable exists for the index, false otherwise.
     */
    boolean iTableExists(Index index);

    /**
     * Checks to see if an iTable exists for the specified index.
     *
     * @param indexId Index Id that you want to check if it has a corresponding
     * iTable.
     * @return True if the iTable exists for the index, false otherwise.
     */
    boolean iTableExists(IndexIdentifier indexId);

}
