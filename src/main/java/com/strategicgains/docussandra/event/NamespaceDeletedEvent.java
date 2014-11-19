package com.strategicgains.docussandra.event;

import com.strategicgains.docussandra.domain.Namespace;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class NamespaceDeletedEvent
extends AbstractEvent<Namespace>
{
	public NamespaceDeletedEvent(Namespace namespace)
	{
		super(namespace);
	}
}
