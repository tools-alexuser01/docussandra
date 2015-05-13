package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Database;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DatabaseCreatedEvent
extends AbstractEvent<Database>
{
	public DatabaseCreatedEvent(Database database)
	{
		super(database);
	}
}
