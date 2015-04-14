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

import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexCreationStatus;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.IndexStatusRepository;
import com.strategicgains.eventing.EventHandler;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles our background indexing tasks.
 * @author udeyoje
 */
public class BackgroundIndexHandler implements EventHandler
{
    
     private static Logger logger = LoggerFactory.getLogger(BackgroundIndexHandler.class);

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
        return eventClass.getName().contains(UUID.class.getName());//TODO: make this better
    }

    @Override
    public void handle(Object event) throws Exception
    {
        logger.debug("Handler recived background indexing event: " + event.toString());
        UUID eventId = (UUID)event;
        IndexCreationStatus status = indexStatusRepo.readEntityByUUID(eventId);
        Index index = indexRepo.read(status.getIndex().getId());
        //docRepo.

    }
    
    
}
