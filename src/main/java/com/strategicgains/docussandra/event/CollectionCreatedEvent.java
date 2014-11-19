package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Collection;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class CollectionCreatedEvent
extends AbstractEvent<Collection>
{
	public CollectionCreatedEvent(Collection collection)
	{
		super(collection);
	}
}
