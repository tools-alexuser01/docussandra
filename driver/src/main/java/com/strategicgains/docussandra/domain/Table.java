package com.strategicgains.docussandra.domain;

import java.util.Iterator;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

public class Table
        extends AbstractTimestampedIdentifiable
{

    @Required("Database")
    @ChildValidation
    private DatabaseReference database;

    @RegexValidation(name = "Collection Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
    private String name;
    private String description;

	//TODO: add consistency & distro metadata here.
    // How long should this data live?
    private long ttl;

    // After delete or update, how long should the old versions live?
    private long deleteTtl;

    public Table()
    {
        super();
    }

    public boolean hasDatabase()
    {
        return (database != null);
    }

    public Database database()
    {
        return database.asObject();
    }

    public void database(Database database)
    {
        this.database = new DatabaseReference(database);
    }

    public void database(String name)
    {
        this.database = new DatabaseReference(name);
    }

    public String databaseName()
    {
        return (hasDatabase() ? database.name() : null);
    }

    public boolean hasName()
    {
        return (name != null);
    }

    public String name()
    {
        return name;
    }

    public void name(String name)
    {
        this.name = name;
    }

    public boolean hasDescription()
    {
        return (description != null);
    }

    public String description()
    {
        return description;
    }

    public void description(String description)
    {
        this.description = description;
    }

    @Override
    public Identifier getId()
    {
        return (hasDatabase() & hasName() ? new Identifier(database.name(), name) : null);
    }

    @Override
    public void setId(Identifier id)
    {
        // do nothing.
    }

    public String toDbTable()
    {
        StringBuilder sb = new StringBuilder();
        Iterator<Object> iter = getId().iterator();
        boolean isFirst = true;

        while (iter.hasNext())
        {
            if (!isFirst)
            {
                sb.append('_');
            } else
            {
                isFirst = false;
            }

            sb.append(iter.next().toString());
        }

        return sb.toString();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(name);

        if (hasDescription())
        {
            sb.append(" (");
            sb.append(description);
            sb.append(")");
        }
        return sb.toString();
    }
}
