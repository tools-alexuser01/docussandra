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
import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

/**
 * POJO that contains the current status of an indexing action.
 *
 * @author udeyoje
 */
public class IndexCreationStatus implements UuidIdentifiable, Serializable
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
     * Estimated time to completion of this index. In seconds. Expect this to be
     * rough.
     */
    private long eta;

    /**
     * Percent complete for this task.
     */
    private double precentComplete;

    /**
     * Status link for this status.
     */
    private String statusLink;

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
     * Error message if an error has occurred in the creation of this index.
     * Will be null if no error has occurred yet.
     */
    private String error;

    /**
     * Default constructor.
     */
    public IndexCreationStatus()
    {
    }

    /**
     * Constructor.
     *
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

    /**
     * Computes any calculated fields.
     */
    public void calculateValues()
    {
        calculatePercentComplete();
        calulateStatusLink();
        calculateEta();
    }

    public boolean isDone()
    {
        return getIndex().isActive();
    }

    /**
     * Calculates out what percent complete this operation is. If more records
     * get added during the operation, the percent complete could decrease
     * instead of climb.
     */
    private void calculatePercentComplete()
    {
        if (getTotalRecords() == 0)
        {
            precentComplete = 100;
        } else if (getRecordsCompleted() == 0)
        {
            precentComplete = 0;
        } else
        {
            precentComplete = (double) ((double) getRecordsCompleted() / (double) getTotalRecords()) * 100d;
        }
    }

    /**
     * Calculates a status link that can be used to check on this request.
     */
    private void calulateStatusLink()
    {
        statusLink = "http://localhost:8081/" + index.databaseName() + "/" + index.tableName() + "/index_status/" + getUuid().toString();//TODO: finish
    }

    private void calculateEta()
    {
        long duration = new Date().getTime() - this.getDateStarted().getTime();
        if (getTotalRecords() == 0)
        {
            eta = 0;//we are functionally done
        } else if (duration == 0)
        {
            //nothing to go off of
            eta = -1;
        } else
        {
            long recordsProcessed = getRecordsCompleted();
            long recordsRemaining = getTotalRecords() - getRecordsCompleted();
            //lets get duration in seconds
            double durationDouble = (double) duration / 1000d;
            double doubleEta = (durationDouble / recordsProcessed) * recordsRemaining;//might need to be recordsProcessed/durationDouble
            eta = (long) doubleEta;
        }
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
     *
     * @return the dateStarted
     */
    public Date getDateStarted()
    {
        return dateStarted;
    }

    /**
     * The date that this request to make an index was issued.
     *
     * @param dateStarted the dateStarted to set
     */
    public void setDateStarted(Date dateStarted)
    {
        this.dateStarted = dateStarted;
    }

    /**
     * The last time this status was updated.
     *
     * @return the statusLastUpdatedAt
     */
    public Date getStatusLastUpdatedAt()
    {
        return statusLastUpdatedAt;
    }

    /**
     * The last time this status was updated.
     *
     * @param statusLastUpdatedAt the statusLastUpdatedAt to set
     */
    public void setStatusLastUpdatedAt(Date statusLastUpdatedAt)
    {
        this.statusLastUpdatedAt = statusLastUpdatedAt;
    }

    /**
     * Estimated time to completion of this index.
     *
     * @return the eta
     */
    public long getEta()
    {
        return eta;
    }

    /**
     * The requested index that is being created.
     *
     * @return the index
     */
    public Index getIndex()
    {
        return index;
    }

    /**
     * The requested index that is being created.
     *
     * @param index the index to set
     */
    public void setIndex(Index index)
    {
        this.index = index;
    }

    /**
     * Total number of records that this index will have when complete.
     *
     * @return the totalRecords
     */
    public long getTotalRecords()
    {
        return totalRecords;
    }

    /**
     * Total number of records that this index will have when complete.
     *
     * @param totalRecords the totalRecords to set
     */
    public void setTotalRecords(long totalRecords)
    {
        this.totalRecords = totalRecords;
    }

    /**
     * Number of records that have been indexed.
     *
     * @return the recordsCompleted
     */
    public long getRecordsCompleted()
    {
        return recordsCompleted;
    }

    /**
     * Number of records that have been indexed.
     *
     * @param recordsCompleted the recordsCompleted to set
     */
    public void setRecordsCompleted(long recordsCompleted)
    {
        this.recordsCompleted = recordsCompleted;
    }

    @Override
    public int hashCode()
    {
        int hash = 3;
        hash = 89 * hash + Objects.hashCode(this.id);
        hash = 89 * hash + Objects.hashCode(this.dateStarted);
        hash = 89 * hash + Objects.hashCode(this.statusLastUpdatedAt);
        hash = 89 * hash + (int) (this.eta ^ (this.eta >>> 32));
        hash = 89 * hash + (int) (Double.doubleToLongBits(this.precentComplete) ^ (Double.doubleToLongBits(this.precentComplete) >>> 32));
        hash = 89 * hash + Objects.hashCode(this.statusLink);
        hash = 89 * hash + Objects.hashCode(this.index);
        hash = 89 * hash + (int) (this.totalRecords ^ (this.totalRecords >>> 32));
        hash = 89 * hash + (int) (this.recordsCompleted ^ (this.recordsCompleted >>> 32));
        hash = 89 * hash + Objects.hashCode(this.error);
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
        final IndexCreationStatus other = (IndexCreationStatus) obj;
        if (!Objects.equals(this.id, other.id))
        {
            return false;
        }
        if (!Objects.equals(this.dateStarted, other.dateStarted))
        {
            return false;
        }
        if (!Objects.equals(this.statusLastUpdatedAt, other.statusLastUpdatedAt))
        {
            return false;
        }
        if (this.eta != other.eta)
        {
            return false;
        }
        if (Double.doubleToLongBits(this.precentComplete) != Double.doubleToLongBits(other.precentComplete))
        {
            return false;
        }
        if (!Objects.equals(this.statusLink, other.statusLink))
        {
            return false;
        }
        if (!Objects.equals(this.index, other.index))
        {
            return false;
        }
        if (this.totalRecords != other.totalRecords)
        {
            return false;
        }
        if (this.recordsCompleted != other.recordsCompleted)
        {
            return false;
        }
        if (!Objects.equals(this.error, other.error))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "IndexCreationStatus{" + "id=" + id + ", dateStarted=" + dateStarted + ", statusLastUpdatedAt=" + statusLastUpdatedAt + ", eta=" + eta + ", precentComplete=" + precentComplete + ", statusLink=" + statusLink + ", index=" + index + ", totalRecords=" + totalRecords + ", recordsCompleted=" + recordsCompleted + ", error=" + error + '}';
    }

    /**
     * Percent complete for this task.
     *
     * @return the precentComplete
     */
    public double getPrecentComplete()
    {
        return precentComplete;
    }

    /**
     * Status link for this status.
     *
     * @return the statusLink
     */
    public String getStatusLink()
    {
        return statusLink;
    }

    /**
     * Error message if an error has occurred in the creation of this index.
     * Will be null if no error has occurred yet.
     *
     * @return the error
     */
    public String getError()
    {
        return error;
    }

    /**
     * Error message if an error has occurred in the creation of this index.
     * Will be null if no error has occurred yet.
     *
     * @param error the error to set
     */
    public void setError(String error)
    {
        this.error = error;
    }

}
