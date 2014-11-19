package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Collection;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class CollectionUpdatedEvent
extends AbstractEvent<Collection>
{
	public CollectionUpdatedEvent(Collection collection)
	{
		super(collection);
	}
}
