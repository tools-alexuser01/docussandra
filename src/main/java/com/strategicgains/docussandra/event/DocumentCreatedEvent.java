package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Document;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DocumentCreatedEvent
extends AbstractEvent<Document>
{
	public DocumentCreatedEvent(Document document)
	{
		super(document);
	}
}
