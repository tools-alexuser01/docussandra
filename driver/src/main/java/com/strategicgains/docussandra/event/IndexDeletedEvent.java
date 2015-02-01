package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Index;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class IndexDeletedEvent
extends AbstractEvent<Index>
{
	public IndexDeletedEvent(Index index)
	{
		super(index);
	}
}
