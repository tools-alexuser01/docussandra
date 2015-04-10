package com.strategicgains.docussandra.domain;

import java.util.Iterator;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;
import java.util.Objects;

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

    @Override
    public int hashCode()
    {
        int hash = 5;
        hash = 59 * hash + Objects.hashCode(this.database);
        hash = 59 * hash + Objects.hashCode(this.name);
        hash = 59 * hash + Objects.hashCode(this.description);
        hash = 59 * hash + (int) (this.ttl ^ (this.ttl >>> 32));
        hash = 59 * hash + (int) (this.deleteTtl ^ (this.deleteTtl >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        final Table other = (Table) obj;
        if (!Objects.equals(this.database, other.database))
        {
            return false;
        }
        if (!Objects.equals(this.name, other.name))
        {
            return false;
        }
        if (!Objects.equals(this.description, other.description))
        {
            return false;
        }
        if (this.ttl != other.ttl)
        {
            return false;
        }
        if (this.deleteTtl != other.deleteTtl)
        {
            return false;
        }
        return true;
    }
    
    
}
