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
package com.strategicgains.docussandra.handler;

import com.datastax.driver.core.BoundStatement;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.event.IndexCreatedEvent;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.IndexStatusRepository;
import com.strategicgains.docussandra.persistence.QueryRepository;
import com.strategicgains.eventing.EventHandler;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles our background indexing tasks.
 *
 * @author udeyoje
 */
public class BackgroundIndexHandler implements EventHandler
{

    private static Logger logger = LoggerFactory.getLogger(BackgroundIndexHandler.class);

    private final static int CHUNK = 1000;

    private IndexRepository indexRepo;
    private IndexStatusRepository indexStatusRepo;
    private DocumentRepository docRepo;

    public BackgroundIndexHandler(IndexRepository indexRepo, IndexStatusRepository indexStatusRepo, DocumentRepository docRepo)
    {
        this.indexRepo = indexRepo;
        this.indexStatusRepo = indexStatusRepo;
        this.docRepo = docRepo;
    }

    @Override
    public boolean handles(Class<?> eventClass)
    {
        return eventClass.equals(IndexCreatedEvent.class);
    }

    @Override
    public void handle(Object event) throws Exception
    {
        logger.debug("Handler recived background indexing event: " + event.toString());
        //Thread.sleep(1000);//pause for a second to ensure the iTable gets created before proceeding (Todd: thoughts on this?)
        IndexCreatedEvent status = null;
        try
        {
            status = (IndexCreatedEvent) event;
            Index index = indexRepo.read(status.getIndex().getId());
            long offset = 0;
            long recordsCompleted = 0;
            QueryResponseWrapper responseWrapper = docRepo.doReadAll(index.databaseName(), index.tableName(), CHUNK, offset);
            boolean hasMore;
            if (responseWrapper.isEmpty())
            {
                hasMore = false;
            } else
            {
                hasMore = true;
            }
            while (hasMore)
            {
                for (Document toIndex : responseWrapper)
                {
                    //actually index here
                    BoundStatement statement = IndexMaintainerHelper.generateDocumentCreateIndexEntryStatement(indexStatusRepo.getSession(), index, toIndex, QueryRepository.getIbl());
                    if (statement != null)
                    {
                        indexStatusRepo.getSession().execute(statement);
                    }
                    recordsCompleted++;
                }
                offset = offset + CHUNK;
                //update status
                status.setRecordsCompleted(recordsCompleted);
                status.setStatusLastUpdatedAt(new Date());
                indexStatusRepo.updateEntity(status);
                //get the next chunk
                responseWrapper = docRepo.doReadAll(index.databaseName(), index.tableName(), CHUNK, offset);
                if (responseWrapper.isEmpty())
                {
                    hasMore = false;
                }
            }
            //update index as done
            index.setActive(true);
            status.setIndex(index);
            indexRepo.markActive(index);
            //final update       
            indexStatusRepo.updateEntity(status);
        } catch (Exception e)
        {
            String indexName  = "Cannot be determined.";
            if(status != null && status.getIndex() != null){
                indexName = status.getIndex().name();
            }
            String errorMessage = "Could not complete indexing event for index: '" + indexName + "'.";
            logger.error(errorMessage, e);
            if(status != null){//intentionally a seperate clause so our error prints in case this throws.
                status.setError(errorMessage + " Please contact a system administrator to resolve this issue.");
                status.setStatusLastUpdatedAt(new Date());
                indexStatusRepo.updateEntity(status);
            }
            throw e;
        }
    }

}
