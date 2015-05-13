package com.strategicgains.docussandra.service;

import java.util.List;

import com.strategicgains.docussandra.domain.XXXCompoundIdentifierEntity;
import com.strategicgains.docussandra.persistence.XXXCompoundIdentifierEntityRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;

/**
 * This is the 'service' or 'business logic' layer, where business logic, syntactic and semantic
 * domain validation occurs, along with calls to the persistence layer.
 */
public class XXXCompoundIdentifierEntityService
{
	private XXXCompoundIdentifierEntityRepository samples;
	
	public XXXCompoundIdentifierEntityService(XXXCompoundIdentifierEntityRepository samplesRepository)
	{
		super();
		this.samples = samplesRepository;
	}

	public XXXCompoundIdentifierEntity create(XXXCompoundIdentifierEntity definition)
	{
		ValidationEngine.validateAndThrow(definition);
		return samples.create(definition);
	}

	public XXXCompoundIdentifierEntity read(Identifier id)
    {
		return samples.read(id);
    }

	public void update(XXXCompoundIdentifierEntity definition)
    {
		ValidationEngine.validateAndThrow(definition);
		samples.update(definition);
    }

	public void delete(Identifier identifier)
    {
		samples.delete(identifier);
    }

	public List<XXXCompoundIdentifierEntity> readAll(String context, String nodeType)
    {
	    return samples.readAll(context, nodeType);
    }

	public long count(String context, String nodeType)
    {
	    return samples.count(context, nodeType);
    }
}
