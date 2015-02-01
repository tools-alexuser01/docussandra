package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Database;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DatabaseUpdatedEvent
extends AbstractEvent<Database>
{
	public DatabaseUpdatedEvent(Database database)
	{
		super(database);
	}
}
