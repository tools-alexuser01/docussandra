package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Document;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DocumentDeletedEvent
extends AbstractEvent<Document>
{
	public DocumentDeletedEvent(Document document)
	{
		super(document);
	}
}
