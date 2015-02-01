package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Table;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class TableCreatedEvent
extends AbstractEvent<Table>
{
	public TableCreatedEvent(Table table)
	{
		super(table);
	}
}
