package com.strategicgains.docussandra.handler;

import com.strategicgains.docussandra.event.IndexCreatedEvent;
import com.strategicgains.eventing.EventHandler;

/**
 * 
 * @author toddf
 * @since Nov 19, 2014
 */
@Deprecated //probably will not be used as logic will probably be handled in the service/obsever classes
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
