package com.strategicgains.docussandra.service;

import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.exception.IndexParseException;
import com.strategicgains.docussandra.exception.ItemNotFoundException;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.TableRepository;
import com.strategicgains.syntaxe.ValidationEngine;
import java.util.UUID;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

public class DocumentService
{

    private TableRepository tables;
    private DocumentRepository docs;
    private final Cache tableCache = CacheFactory.getCache("tableExist");

    public DocumentService(TableRepository databaseRepository, DocumentRepository documentRepository)
    {
        super();
        this.docs = documentRepository;
        this.tables = databaseRepository;
    }

    public Document create(String database, String table, String json) throws IndexParseException
    {
        verifyTable(database, table);

        Document doc = new Document();
        doc.table(database, table);
        doc.object(json);
        doc.setUuid(UUID.randomUUID());//TODO: is this right?
        ValidationEngine.validateAndThrow(doc);
        try
        {
            return docs.doCreate(doc);
        } catch (RuntimeException e)//the framework does not allow us to throw the IndexParseException directly from the repository layer
        {
            if (e.getCause() != null && e.getCause() instanceof IndexParseException)
            {
                throw (IndexParseException) e.getCause();
            } else
            {
                throw e;
            }
        }
    }

    public Document read(String database, String table, Identifier id)
    {
        verifyTable(database, table);
        return docs.doRead(id);
    }

//	public List<Document> readAll(String database, String table)
//	{
//		verifyTable(database, table);
//		return docs.readAll(database, table);
//	}
//
//	public long countAll(String database, String table)
//	{
//		return docs.countAll(database, table);
//	}
    public void update(Document entity)
    {
        ValidationEngine.validateAndThrow(entity);
        docs.doUpdate(entity);
    }

    public void delete(String database, String table, Identifier id)
    {
        verifyTable(database, table);
        docs.doDelete(id);
    }

    private void verifyTable(String database, String table)
    {
        String key = database + table;
        Identifier tableId = new Identifier(database, table);
//        synchronized (CacheSynchronizer.getLockingObject(key, "tableExist"))
//        {
        Element e = tableCache.get(key);
        if (e == null || e.getObjectValue() == null)//if its not set, or set, but null, re-read
        {
            //not cached; let's read it                        
            e = new Element(key, (Boolean) tables.exists(tableId));
        }
        if (!(Boolean) e.getObjectValue())
        {
            throw new ItemNotFoundException("Table not found: " + tableId.toString());
        }
//        }
//        if (!tables.exists(tableId)){
//            throw new ItemNotFoundException("Table not found: " + tableId.toString());
//        }
    }
}
