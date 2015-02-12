package com.strategicgains.docussandra.persistence;

import com.strategicgains.docussandra.event.EventFactory;
import com.strategicgains.eventing.DomainEvents;
import com.strategicgains.repoexpress.domain.Identifiable;
import com.strategicgains.repoexpress.event.AbstractRepositoryObserver;

/**
 * @author udeyoje
 * @since Feb 12, 2015
 */
public class IndexChangeObserver<T extends Identifiable>
extends AbstractRepositoryObserver<T>
{
	private EventFactory<T> factory;

	public IndexChangeObserver(EventFactory<T> eventFactory)
	{
		super();
		this.factory = eventFactory;
	}

	@Override
    public void afterCreate(T object)
    {
		if (factory != null)
		{
			publish(factory.newCreatedEvent(object));
		}
    }

	@Override
    public void afterDelete(T object)
    {
		if (factory != null)
		{
			publish(factory.newDeletedEvent(object));
		}
    }

	@Override
    public void afterUpdate(T object)
    {
		if (factory != null)
		{
			publish(factory.newUpdatedEvent(object));
		}
    }

	private void publish(Object event)
    {
		if (event == null) return;

		DomainEvents.publish(event);
    }
}
