package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Namespace;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class NamespaceCreatedEvent
extends AbstractEvent<Namespace>
{
	public NamespaceCreatedEvent(Namespace namespace)
	{
		super(namespace);
	}
}
