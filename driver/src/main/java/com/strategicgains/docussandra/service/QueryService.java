package com.strategicgains.docussandra.service;

import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.persistence.QueryDao;

public class QueryService
{
	private QueryDao queries;
	
	public QueryService(QueryDao queryRepository)
	{
		super();
		this.queries = queryRepository;
	}
        
        public Object query(Query toQuery){
            return queries.doQuery(toQuery);
        }
//
//	public Query create(Query entity)
//	{
//		ValidationEngine.validateAndThrow(entity);
//		return queries.create(entity);
//	}
//
//	public Query read(Identifier id)
//    {
//		return queries.read(id);
//    }
//
//	public void update(Query entity)
//    {
//		ValidationEngine.validateAndThrow(entity);
//		queries.update(entity);
//    }
//
//	public void delete(Identifier id)
//    {
//		queries.delete(id);
//    }
}
