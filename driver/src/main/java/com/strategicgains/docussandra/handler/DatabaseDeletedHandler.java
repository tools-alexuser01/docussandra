package com.strategicgains.docussandra.handler;

import com.strategicgains.docussandra.event.DatabaseDeletedEvent;
import com.strategicgains.eventing.EventHandler;

/**
 * 
 * @author toddf
 * @since Nov 19, 2014
 */
public class DatabaseDeletedHandler
implements EventHandler
{
	@Override
	public void handle(Object event) throws Exception
	{
		handle((DatabaseDeletedEvent) event);
	}

	public void handle(DatabaseDeletedEvent event)
	{
		//TODO: remove all the collections and all the documents in that database.  
	}

	@Override
	public boolean handles(Class<?> eventClass)
	{
		return DatabaseDeletedEvent.class.isAssignableFrom(eventClass);
	}
}
