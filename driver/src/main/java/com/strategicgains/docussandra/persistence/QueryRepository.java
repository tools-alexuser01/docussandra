package com.strategicgains.docussandra.persistence;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.Utils;
import com.strategicgains.docussandra.bucketmanagement.IndexBucketLocator;
import com.strategicgains.docussandra.bucketmanagement.SimpleIndexBucketLocatorImpl;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.ParsedQuery;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryRepository
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String QUERY_CQL = "select * from %s where bucket = ? AND %s";
    private static final String QUERY_CQL_LIMIT = "select * from %s where bucket = ? AND %s LIMIT %s";//we use the limit as to not put any more stress on cassandra than we need to (even though our algorithm will discard the data anyway)

    private IndexBucketLocator ibl = new SimpleIndexBucketLocatorImpl(200);

    private Session session;

    public QueryRepository(Session session)
    {
        this.session = session;
    }

    private BoundStatement generateQueryStatement(ParsedQuery query, long maxIndex)
    {
        String finalQuery;
        //format QUERY_CQL
        if (maxIndex == -1)//no artifical limit
        {
            finalQuery = String.format(QUERY_CQL, query.getITable(), query.getWhereClause().getBoundStatementSyntax());
        } else //with a limit
        {
            finalQuery = String.format(QUERY_CQL_LIMIT, query.getITable(), query.getWhereClause().getBoundStatementSyntax(), maxIndex);
        }
        //run query
        PreparedStatement ps = session.prepare(finalQuery);//TODO: Cache
        BoundStatement bs = new BoundStatement(ps);
        //set the bucket
        UUID fuzzyUUID = Utils.convertStringToFuzzyUUID(query.getWhereClause().getValues().get(0));//fuzzy UUID is based on first field value
        String bucket = ibl.getBucket(null, fuzzyUUID);
        bs.setString(0, bucket);
        int i = 1;
        for (String bindValue : query.getWhereClause().getValues())//no great reason for not using the other loop format
        {
            bs.setString(i, bindValue);
            i++;
        }
        return bs;
    }

    public List<Document> doQuery(ParsedQuery query)
    {
        //run the query
        ResultSet results = session.execute(generateQueryStatement(query, -1));
        //process result(s)
        ArrayList<Document> toReturn = new ArrayList<>();
        Iterator<Row> ite = results.iterator();
        while (ite.hasNext())
        {
            Row row = ite.next();
            toReturn.add(DocumentRepository.marshalRow(row));
        }
        return toReturn;
    }

    public List<Document> doQuery(ParsedQuery query, int limit, long offset)
    {
        long maxIndex = offset + limit;
        //run the query
        ResultSet results = session.execute(generateQueryStatement(query, maxIndex));
        //process result(s)
        ArrayList<Document> toReturn = new ArrayList<>(limit);
        Iterator<Row> ite = results.iterator();
        long offsetCounter = 0;
        while (ite.hasNext())//for each item in the result set
        {
            Row row = ite.next();
            if (offsetCounter >= maxIndex)//if we are at a counter less than our max amount to return (offset + limit)
            {
                break;//we are done; don't bother processing anymore, it's not going to be used anyway
            } else if (offsetCounter >= offset)//if we are at a counter greater than or equal to our offset -- we are in the sweet spot of the result set to return
            {
                toReturn.add(DocumentRepository.marshalRow(row));//we can add it to our return list
            } else
            {
                if (logger.isTraceEnabled())
                {
                    logger.trace("We are probably wasting processor time by processing a query inefficently");//TODO: obviously, consider improving this (or at least take out the logger if we decide not to)
                }
            }
            offsetCounter++;

        }
        return toReturn;
    }

    /**
     * @return the session
     */
    public Session getSession()
    {
        return session;
    }

}
