package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Index;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class IndexUpdatedEvent
extends AbstractEvent<Index>
{
	public IndexUpdatedEvent(Index index)
	{
		super(index);
	}
}
