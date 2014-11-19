package com.strategicgains.docussandra.handler;

import com.strategicgains.docussandra.event.IndexDeletedEvent;
import com.strategicgains.eventing.EventHandler;

/**
 * 
 * @author toddf
 * @since Nov 19, 2014
 */
public class IndexDeletedHandler
implements EventHandler
{
	@Override
	public void handle(Object event) throws Exception
	{
		handle((IndexDeletedEvent) event);
	}

	public void handle(IndexDeletedEvent event)
	{
		//TODO: remove the index table(s).  
	}

	@Override
	public boolean handles(Class<?> eventClass)
	{
		return IndexDeletedEvent.class.isAssignableFrom(eventClass);
	}
}
