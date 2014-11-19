package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Namespace;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class NamespaceUpdatedEvent
extends AbstractEvent<Namespace>
{
	public NamespaceUpdatedEvent(Namespace namespace)
	{
		super(namespace);
	}
}
