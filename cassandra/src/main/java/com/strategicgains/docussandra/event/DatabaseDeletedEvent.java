package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Database;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
@Deprecated
public class DatabaseDeletedEvent
extends AbstractEvent<Database>
{
	public DatabaseDeletedEvent(Database database)
	{
		super(database);
	}
}
