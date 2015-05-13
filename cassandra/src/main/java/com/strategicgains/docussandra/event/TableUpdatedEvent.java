package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Table;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class TableUpdatedEvent
extends AbstractEvent<Table>
{
	public TableUpdatedEvent(Table table)
	{
		super(table);
	}
}
