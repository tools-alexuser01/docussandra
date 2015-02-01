package com.strategicgains.docussandra.handler;

import com.strategicgains.docussandra.event.IndexCreatedEvent;
import com.strategicgains.eventing.EventHandler;

/**
 * 
 * @author toddf
 * @since Nov 19, 2014
 */
public class IndexCreatedHandler
implements EventHandler
{
	@Override
	public void handle(Object event) throws Exception
	{
		handle((IndexCreatedEvent) event);
	}

	public void handle(IndexCreatedEvent event)
	{
		//TODO: index all the documents in the collection.  
	}

	@Override
	public boolean handles(Class<?> eventClass)
	{
		return IndexCreatedEvent.class.isAssignableFrom(eventClass);
	}
}
