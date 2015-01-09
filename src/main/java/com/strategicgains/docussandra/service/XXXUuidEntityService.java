package com.strategicgains.docussandra.service;

import com.strategicgains.docussandra.domain.XXXUuidEntity;
import com.strategicgains.docussandra.persistence.XXXUuidEntityRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;

/**
 * This is the 'service' or 'business logic' layer, where business logic, syntactic and semantic
 * domain validation occurs, along with calls to the persistence layer.
 */
public class XXXUuidEntityService
{
	private XXXUuidEntityRepository samples;
	
	public XXXUuidEntityService(XXXUuidEntityRepository samplesRepository)
	{
		super();
		this.samples = samplesRepository;
	}

	public XXXUuidEntity create(XXXUuidEntity entity)
	{
		ValidationEngine.validateAndThrow(entity);
		return samples.create(entity);
	}

	public XXXUuidEntity read(Identifier id)
    {
		return samples.read(id);
    }

	public void update(XXXUuidEntity entity)
    {
		ValidationEngine.validateAndThrow(entity);
		samples.update(entity);
    }

	public void delete(Identifier id)
    {
		samples.delete(id);
    }
}
