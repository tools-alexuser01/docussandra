package com.strategicgains.docussandra.service;

import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.ParsedQuery;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.WhereClause;
import com.strategicgains.docussandra.exception.FieldNotIndexedException;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.QueryRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Service for performing a query.
 * @author udeyoje
 */
public class QueryService
{

    /**
     * Query Repository for accessing the database.
     */
    private QueryRepository queries;

    /**
     * Constructor. 
     * @param queryRepository QueryRepository to use to perform the query.
     */
    public QueryService(QueryRepository queryRepository)
    {
        super();
        this.queries = queryRepository;
    }

    /**
     * Does a query with no limit or offset.
     * @param db Database to query.
     * @param toQuery Query perform.
     * @return 
     */
    public List<Document> query(String db, Query toQuery)
    {
        ParsedQuery parsedQuery = parseQuery(db, toQuery);//note: throws a runtime exception
        return queries.doQuery(parsedQuery);
    }

    public List<Document> query(String db, Query toQuery, int limit, long offset)
    {
        ParsedQuery parsedQuery = parseQuery(db, toQuery);//note: throws a runtime exception
        return queries.doQuery(parsedQuery, limit, offset);
    }
    
    private ParsedQuery getParsedQuery(String db, Query toParse){
        return parseQuery(db, toParse);
    }
    

    /**
     * Parses a query to determine if it is valid and determine the information
     * we actually need to perform the query.
     *
     * @param db Database that the query will run against
     * @param toParse
     * @return A ParsedQuery object for the query.
     * @throws FieldNotIndexedException
     */
    //TODO: cache the results of this method; will be expensive and very frequent
    public ParsedQuery parseQuery(String db, Query toParse) throws FieldNotIndexedException
    {
        //let's parse the where clause so we know what we are actually searching for
        WhereClause where = new WhereClause(toParse.getWhere());
        //determine if the query is valid; in other words is it searching on valid fields that we have indexed
        List<String> fieldsToQueryOn = where.getFields();
        IndexRepository indexRepo = new IndexRepository(queries.getSession());
        List<Index> indices = indexRepo.readAll(db, toParse.getTable());
        Index indexToUse = null;
        for (Index index : indices)
        {
            if (equalLists(index.fields(), fieldsToQueryOn))
            {
                indexToUse = index;//we have a perfect match; the index matches the query exactly
                break;
            }
        }
        if (indexToUse == null)
        {//whoops, no perfect match, let try for a partial match (ie, the index has more fields than the query)
            //TODO: querying on non-primary fields will lead to us being unable to determine which bucket to search -- give the user an override option, but for now just throw an exception
            for (Index index : indices)
            {
                //make a copy of the fieldsToQueryOn so we don't mutate the orginal
                ArrayList<String> fieldsToQueryOnCopy = new ArrayList<>(fieldsToQueryOn);
                ArrayList<String> indexFields = new ArrayList<>(index.fields());//make a copy here too
                fieldsToQueryOnCopy.removeAll(indexFields);//we remove all the fields we have, from the fields we want
                //if there are not any fields left in fields we want
                if (fieldsToQueryOnCopy.isEmpty() && fieldsToQueryOn.contains(indexFields.get(0)))
                {//second clause in this statement is what ensure we have a primary index; see TODO above.
                    //we have an index that will work (even though we have extra fields in it)
                    indexToUse = index;
                    break;
                }
            }
        }
        if (indexToUse == null)
        {
            throw new FieldNotIndexedException(fieldsToQueryOn);
        }
        ParsedQuery toReturn = new ParsedQuery(toParse, where, Utils.calculateITableName(indexToUse));
        return toReturn;
    }

    //TODO: move this to PearsonLibrary
    public boolean equalLists(List<String> one, List<String> two)
    {
        if (one == null && two == null)
        {
            return true;
        }

        if ((one == null && two != null)
                || one != null && two == null
                || one.size() != two.size())
        {
            return false;
        }

        //to avoid messing the order of the lists we will use a copy
        ArrayList<String> oneCopy = new ArrayList<>(one);
        ArrayList<String> twoCopy = new ArrayList<>(two);

        Collections.sort(oneCopy);
        Collections.sort(twoCopy);
        return one.equals(twoCopy);
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
