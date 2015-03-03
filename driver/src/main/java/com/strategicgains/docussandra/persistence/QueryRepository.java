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

public class QueryRepository
{

    private static final String QUERY_CQL = "select * from %s where bucket = ? AND %s";

    private IndexBucketLocator ibl = new SimpleIndexBucketLocatorImpl(200);

    private Session session;

    public QueryRepository(Session session)
    {
        //super(session, "queries", "id");
        this.session = session;
        //initializeStatements();
    }

    public List<Document> doQuery(String db, ParsedQuery query)
    {
        //format QUERY_CQL
        String finalQuery = String.format(QUERY_CQL, query.getITable(), query.getWhereClause().getBoundStatementSyntax());
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
        //run the query
        ResultSet results = session.execute(bs);
        //process result(s)
        //right now we just are going go return a list of documents
        ArrayList<Document> toReturn = new ArrayList<>();
        Iterator<Row> ite = results.iterator();
        while (ite.hasNext())
        {
            Row row = ite.next();
            toReturn.add(DocumentRepository.marshalRow(row));
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
