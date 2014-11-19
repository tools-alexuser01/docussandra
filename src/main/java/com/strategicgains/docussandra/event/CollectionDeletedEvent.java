package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Collection;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class CollectionDeletedEvent
extends AbstractEvent<Collection>
{
	public CollectionDeletedEvent(Collection collection)
	{
		super(collection);
	}
}
