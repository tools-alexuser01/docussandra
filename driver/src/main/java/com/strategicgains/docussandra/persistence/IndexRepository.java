package com.strategicgains.docussandra.persistence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.cache.CacheSynchronizer;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.docussandra.event.EventFactory;
import com.strategicgains.docussandra.event.IndexCreatedEvent;
import com.strategicgains.docussandra.event.IndexDeletedEvent;
import com.strategicgains.docussandra.event.IndexUpdatedEvent;
import com.strategicgains.docussandra.persistence.helper.PreparedStatementFactory;
import com.strategicgains.repoexpress.cassandra.AbstractCassandraRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.event.DefaultTimestampedIdentifiableRepositoryObserver;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexRepository
        extends AbstractCassandraRepository<Index>
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private class Tables
    {

        static final String BY_ID = "sys_idx";
    }

    private class Columns
    {

        static final String DATABASE = "db_name";
        static final String TABLE = "tbl_name";
        static final String NAME = "name";
        static final String IS_UNIQUE = "is_unique";
        static final String BUCKET_SIZE = "bucket_sz";
        static final String FIELDS = "fields";
        static final String ONLY = "only";
        static final String CREATED_AT = "created_at";
        static final String UPDATED_AT = "updated_at";
    }

    private static final String IDENTITY_CQL = " where db_name = ? and tbl_name = ? and name = ?";
    private static final String EXISTENCE_CQL = "select count(*) from %s" + IDENTITY_CQL;
    private static final String CREATE_CQL = "insert into %s (%s, db_name, tbl_name, is_unique, bucket_sz, fields, only, created_at, updated_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String READ_CQL = "select * from %s" + IDENTITY_CQL;
    private static final String DELETE_CQL = "delete from %s" + IDENTITY_CQL;
    private static final String UPDATE_CQL = "update %s set bucket_sz = ?, updated_at = ?" + IDENTITY_CQL;
    private static final String READ_ALL_CQL = "select * from %s where db_name = ? and tbl_name = ?";
    private static final String READ_ALL_COUNT_CQL = "select count(*) from %s where db_name = ? and tbl_name = ?";

    private PreparedStatement existStmt;
    private PreparedStatement readStmt;
    private PreparedStatement createStmt;
    private PreparedStatement deleteStmt;
    private PreparedStatement updateStmt;
    private PreparedStatement readAllStmt;
    private PreparedStatement readAllCountStmt;

    public IndexRepository(Session session)
    {
        super(session, Tables.BY_ID);
        addObserver(new DefaultTimestampedIdentifiableRepositoryObserver<Index>());
        addObserver(new StateChangeEventingObserver<Index>(new IndexEventFactory()));
        addObserver(new IndexChangeObserver(session));
        initialize();
    }

    protected void initialize()
    {
        existStmt = PreparedStatementFactory.getPreparedStatement(String.format(EXISTENCE_CQL, getTable()), getSession());
        readStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_CQL, getTable()), getSession());
        createStmt = PreparedStatementFactory.getPreparedStatement(String.format(CREATE_CQL, getTable(), Columns.NAME), getSession());
        deleteStmt = PreparedStatementFactory.getPreparedStatement(String.format(DELETE_CQL, getTable()), getSession());
        updateStmt = PreparedStatementFactory.getPreparedStatement(String.format(UPDATE_CQL, getTable()), getSession());
        readAllStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_CQL, getTable()), getSession());
        readAllCountStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_COUNT_CQL, getTable()), getSession());
    }

    @Override
    public boolean exists(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return false;
        }

        BoundStatement bs = new BoundStatement(existStmt);
        bindIdentifier(bs, identifier);
        return (getSession().execute(bs).one().getLong(0) > 0);
    }

    @Override
    protected Index readEntityById(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return null;
        }

        BoundStatement bs = new BoundStatement(readStmt);
        bindIdentifier(bs, identifier);
        return marshalRow(getSession().execute(bs).one());
    }

    @Override
    protected Index createEntity(Index entity)
    {
        BoundStatement bs = new BoundStatement(createStmt);
        bindCreate(bs, entity);
        getSession().execute(bs);
        //maintain cache -- TODO: break into seperate method; probably a new thread        
        try//we do this in a try/catch because we don't want to cause an app error if this fails
        {
            Cache c = CacheFactory.getCache("index");
            String key = entity.databaseName() + ":" + entity.tableName();
            synchronized (CacheSynchronizer.getLockingObject(key, Index.class))
            {
                List<Index> currentIndex = this.readAll(entity.databaseName(), entity.tableName());
                Element e = new Element(key, currentIndex);
                c.put(e);
            }
        } catch (Exception e)
        {
            logger.error("Could not update index cache upon index create.", e);
        }
        return entity;
    }

    @Override
    protected Index updateEntity(Index entity)
    {
        throw new UnsupportedOperationException("Updates are not supported on indices; create a new one and delete the old one if you would like this functionality.");
//		BoundStatement bs = new BoundStatement(updateStmt);
//		bindUpdate(bs, entity);
//		getSession().execute(bs);
//		return entity;
    }

    @Override
    protected void deleteEntity(Index entity)
    {
        BoundStatement bs = new BoundStatement(deleteStmt);
        bindIdentifier(bs, entity.getId());
        getSession().execute(bs);
        //maintain cache
        try//we do this in a try/catch because we don't want to cause an app error if this fails
        {
            Cache c = CacheFactory.getCache("index");
            String key = entity.databaseName() + ":" + entity.tableName();
            synchronized (CacheSynchronizer.getLockingObject(key, Index.class))
            {
                List<Index> currentIndex = this.readAll(entity.databaseName(), entity.tableName());
                if (!currentIndex.isEmpty())
                {
                    Element e = new Element(key, currentIndex);
                    c.put(e);
                } else
                {
                    c.put(null);
                }
            }
        } catch (Exception e)
        {
            logger.error("Could not update index cache upon index delete.", e);
        }
    }

    public List<Index> readAll(String namespace, String collection)
    {
        BoundStatement bs = new BoundStatement(readAllStmt);
        bs.bind(namespace, collection);
        return (marshalAll(getSession().execute(bs)));
    }

    /**
     * Same as readAll, but will read from the cache if availible.
     *
     * @param namespace
     * @param collection
     * @return
     */
    public List<Index> readAllCached(String namespace, String collection)
    {
        String key = namespace + ":" + collection;
        //logger.info("Reading all indexes from cache for " + key);
        Cache c = CacheFactory.getCache("index");
        synchronized (CacheSynchronizer.getLockingObject(key, Index.class))
        {
            Element e = c.get(key);
            if (e == null || e.getObjectValue() == null)//if its not set, or set, but null, re-read
            {
                e = new Element(key, readAll(namespace, collection));
                c.put(e);
            } else
            {
                logger.trace("Pulling Index from Cache: " + e.getObjectValue().toString());
            }
            return (List<Index>) e.getObjectValue();
        }
        //return readAll(namespace, collection);
    }

    public long countAll(String namespace, String collection)
    {
        BoundStatement bs = new BoundStatement(readAllCountStmt);
        bs.bind(namespace, collection);
        return (getSession().execute(bs).one().getLong(0));
    }

    private void bindCreate(BoundStatement bs, Index entity)
    {
        bs.bind(entity.name(),
                entity.databaseName(),
                entity.tableName(),
                entity.isUnique(),
                entity.bucketSize(),
                entity.fields(),
                entity.includeOnly(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }

    private void bindUpdate(BoundStatement bs, Index entity)
    {
        bs.bind(entity.bucketSize(),
                entity.getUpdatedAt(),
                entity.databaseName(),
                entity.tableName(),
                entity.name());
    }

    private List<Index> marshalAll(ResultSet rs)
    {
        List<Index> indexes = new ArrayList<Index>();
        Iterator<Row> i = rs.iterator();

        while (i.hasNext())
        {
            indexes.add(marshalRow(i.next()));
        }

        return indexes;
    }

    protected Index marshalRow(Row row)
    {
        if (row == null)
        {
            return null;
        }

        Index i = new Index();
        i.name(row.getString(Columns.NAME));
        Table t = new Table();
        t.database(row.getString(Columns.DATABASE));
        t.name(row.getString(Columns.TABLE));
        i.table(t);
        i.isUnique(row.getBool(Columns.IS_UNIQUE));
        i.bucketSize(row.getLong(Columns.BUCKET_SIZE));
        i.fields(row.getList(Columns.FIELDS, String.class));
        i.includeOnly(row.getList(Columns.ONLY, String.class));
        i.setCreatedAt(row.getDate(Columns.CREATED_AT));
        i.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
        return i;
    }

    private class IndexEventFactory
            implements EventFactory<Index>
    {

        @Override
        public Object newCreatedEvent(Index object)
        {
            return new IndexCreatedEvent(object);
        }

        @Override
        public Object newUpdatedEvent(Index object)
        {
            return new IndexUpdatedEvent(object);
        }

        @Override
        public Object newDeletedEvent(Index object)
        {
            return new IndexDeletedEvent(object);
        }
    }
}
