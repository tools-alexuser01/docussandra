package com.strategicgains.docussandra.service;

import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.QueryDao;
import java.util.List;

public class QueryService {

    private QueryDao queries;

    public QueryService(QueryDao queryRepository) {
        super();
        this.queries = queryRepository;
    }

    public List<Document> query(String db, Query toQuery) {
        String table = validateQueryAndDetermineTable(db, toQuery);
        if (table == null) {
            //TODO: throw a new FieldNotIndexedException; include the bad field somehow
            throw new RuntimeException("Field not indexed; query not possible");
        } else {
            return queries.doQuery(db, toQuery, table);
        }
    }

    /**
     * Parses a query to determine if it is valid and which table we need to
     * query on. Will return null if the query is not valid.
     *
     * @param db Database that the query will run against
     * @param toValidate Query to be validated.
     * @return A string of the table name that the query will need to execute
     * against. Null if the query is not valid.
     */
    public String validateQueryAndDetermineTable(String db, Query toValidate) {
        //determine if the query is valid; in other words is it searching on valid fields that we have indexed
        IndexRepository indexRepo = new IndexRepository(queries.getSession());
        List<Index> indices = indexRepo.readAll(db, toValidate.getTable());

        //determine which iTable(s) we need to query on -- the field could be in more than one iTable, so which one do we pick? the one with the least number of additional fields?
        return null;
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
