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
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryRepository
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String QUERY_CQL = "select * from %s where bucket = ? AND %s";
    private static final String QUERY_CQL_LIMIT = "select * from %s where bucket = ? AND %s LIMIT %s";//we use the limit as to not put any more stress on cassandra than we need to (even though our algorithm will discard the data anyway)

    private static IndexBucketLocator ibl = new SimpleIndexBucketLocatorImpl(200);

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
        PreparedStatement ps = PreparedStatementFactory.getPreparedStatement(finalQuery, getSession());
        BoundStatement bs = new BoundStatement(ps);
        //set the bucket
        UUID fuzzyUUID = Utils.convertStringToFuzzyUUID(query.getWhereClause().getValues().get(0));//fuzzy UUID is based on first field value
        String bucket = getIbl().getBucket(null, fuzzyUUID);
        bs.setString(0, bucket);
        int i = 1;
        for (String bindValue : query.getWhereClause().getValues())
        {
            bs.setString(i, bindValue);
            i++;
        }
        return bs;
    }

    public QueryResponseWrapper doQuery(ParsedQuery query)
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
        return new QueryResponseWrapper(toReturn, 0l);
    }

    public QueryResponseWrapper doQuery(ParsedQuery query, int limit, long offset)
    {        
        //run the query
        long maxIndex = offset + limit;
        ResultSet results = session.execute(generateQueryStatement(query, maxIndex + 1));//we do one plus here so we know if there are additional results
        return DocumentRepository.parseResultSetWithLimitAndOffset(results, limit, offset);
    }

    /**
     * @return the session
     */
    public Session getSession()
    {
        return session;
    }

    /**
     * @return the ibl
     */
    public static IndexBucketLocator getIbl()
    {
        return ibl;
    }

}
