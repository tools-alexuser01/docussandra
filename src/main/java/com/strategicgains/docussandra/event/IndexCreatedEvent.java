package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Index;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class IndexCreatedEvent
extends AbstractEvent<Index>
{
	public IndexCreatedEvent(Index index)
	{
		super(index);
	}
}
