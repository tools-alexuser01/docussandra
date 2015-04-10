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
package com.strategicgains.docussandra.domain;

import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.domain.UuidIdentifiable;
import java.util.Date;
import java.util.UUID;

/**
 * POJO that contains the current status of an indexing action.
 *
 * @author udeyoje
 */
public class IndexCreationStatus implements UuidIdentifiable
{

    /**
     * UUID for this object.
     */
    private UUID id;

    /**
     * The date that this request to make an index was issued.
     */
    private Date dateStarted;

    /**
     * The last time this status was updated.
     */
    private Date statusLastUpdatedAt;

    /**
     * Estimated time to completion of this index.
     */
    private long eta;//likely a pipe dream, but it would be sweet if this worked

    /**
     * The requested index that is being created.
     */
    private Index index;

    /**
     * Total number of records that this index will have when complete.
     */
    private long totalRecords;

    /**
     * Number of records that have been indexed.
     */
    private long recordsCompleted;

    /**
     * Default constructor.
     */
    public IndexCreationStatus()
    {
    }

    /**
     * Constructor.
     * @param id
     * @param dateStarted
     * @param statusLastUpdatedAt
     * @param index
     * @param totalRecords
     * @param recordsCompleted 
     */
    public IndexCreationStatus(UUID id, Date dateStarted, Date statusLastUpdatedAt, Index index, long totalRecords, long recordsCompleted)
    {
        this.id = id;
        this.dateStarted = dateStarted;
        this.statusLastUpdatedAt = statusLastUpdatedAt;
        this.index = index;
        this.totalRecords = totalRecords;
        this.recordsCompleted = recordsCompleted;
    }

    
    public boolean isDone()
    {
        return getIndex().isActive();
    }

    /**
     * Calculates out what percent complete this operation is. If more records
     * get added during the operation, the percent complete could decrease
     * instead of climb.
     *
     * @return The current percent complete of this index creation operation.
     */
    public double calculatePercentComplete()
    {
        return (double) ((double) getRecordsCompleted() / (double) getTotalRecords()) * 100d;
    }
    
    /**
     * Gets a status link that can be used to check on this request.
     * @return 
     */
    public String getStatusLink(){
        return ""; //TODO: finish
    }

    @Override
    public UUID getUuid()
    {
        return id;
    }

    @Override
    public void setUuid(UUID id)
    {
        this.id = id;
    }

    @Override
    public Identifier getId()
    {
        return new Identifier(getIndex().databaseName(), getIndex().tableName(), getIndex().name(), id);
    }

    @Override
    public void setId(Identifier id)
    {
        // Do nothing. Throw.
        throw new UnsupportedOperationException("This is not a valid call for this object.");
    }

    /**
     * The date that this request to make an index was issued.
     * @return the dateStarted
     */
    public Date getDateStarted()
    {
        return dateStarted;
    }

    /**
     * The date that this request to make an index was issued.
     * @param dateStarted the dateStarted to set
     */
    public void setDateStarted(Date dateStarted)
    {
        this.dateStarted = dateStarted;
    }

    /**
     * The last time this status was updated.
     * @return the statusLastUpdatedAt
     */
    public Date getStatusLastUpdatedAt()
    {
        return statusLastUpdatedAt;
    }

    /**
     * The last time this status was updated.
     * @param statusLastUpdatedAt the statusLastUpdatedAt to set
     */
    public void setStatusLastUpdatedAt(Date statusLastUpdatedAt)
    {
        this.statusLastUpdatedAt = statusLastUpdatedAt;
    }

    /**
     * Estimated time to completion of this index.
     * @return the eta
     */
    public long getEta()
    {
        return eta;
    }

    /**
     * Estimated time to completion of this index.
     * @param eta the eta to set
     */
    public void setEta(long eta)
    {
        this.eta = eta;
    }

    /**
     * The requested index that is being created.
     * @return the index
     */
    public Index getIndex()
    {
        return index;
    }

    /**
     * The requested index that is being created.
     * @param index the index to set
     */
    public void setIndex(Index index)
    {
        this.index = index;
    }

    /**
     * Total number of records that this index will have when complete.
     * @return the totalRecords
     */
    public long getTotalRecords()
    {
        return totalRecords;
    }

    /**
     * Total number of records that this index will have when complete.
     * @param totalRecords the totalRecords to set
     */
    public void setTotalRecords(long totalRecords)
    {
        this.totalRecords = totalRecords;
    }

    /**
     * Number of records that have been indexed.
     * @return the recordsCompleted
     */
    public long getRecordsCompleted()
    {
        return recordsCompleted;
    }

    /**
     * Number of records that have been indexed.
     * @param recordsCompleted the recordsCompleted to set
     */
    public void setRecordsCompleted(long recordsCompleted)
    {
        this.recordsCompleted = recordsCompleted;
    }
}
