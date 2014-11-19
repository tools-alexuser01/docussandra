package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Document;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DocumentUpdatedEvent
extends AbstractEvent<Document>
{
	public DocumentUpdatedEvent(Document document)
	{
		super(document);
	}
}
