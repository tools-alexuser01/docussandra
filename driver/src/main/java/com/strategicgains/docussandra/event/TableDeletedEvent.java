package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Table;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class TableDeletedEvent
extends AbstractEvent<Table>
{
	public TableDeletedEvent(Table table)
	{
		super(table);
	}
}
