package com.strategicgains.docussandra.handler;

import com.strategicgains.docussandra.event.NamespaceDeletedEvent;
import com.strategicgains.eventing.EventHandler;

/**
 * 
 * @author toddf
 * @since Nov 19, 2014
 */
public class NamespaceDeletedHandler
implements EventHandler
{
	@Override
	public void handle(Object event) throws Exception
	{
		handle((NamespaceDeletedEvent) event);
	}

	public void handle(NamespaceDeletedEvent event)
	{
		//TODO: remove all the collections and all the documents in that namespace.  
	}

	@Override
	public boolean handles(Class<?> eventClass)
	{
		return NamespaceDeletedEvent.class.isAssignableFrom(eventClass);
	}
}
