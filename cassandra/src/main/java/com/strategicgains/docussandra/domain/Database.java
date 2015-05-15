package com.strategicgains.docussandra.domain;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.docussandra.domain.parent.Identifiable;
import com.strategicgains.docussandra.domain.parent.Timestamped;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import java.util.Objects;
import org.restexpress.plugin.hyperexpress.Linkable;

public class Database
        extends Timestamped implements Linkable, Identifiable
{

    @RegexValidation(name = "Namespace Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
    private String name;
    private String description;

	//TODO: add consistency & distro metadata here.
    public Database()
    {
        super();
    }

    public Database(String name)
    {
        this();
        name(name);
    }

    public String name()
    {
        return name;
    }

    public void name(String name)
    {
        this.name = name;
    }

    @Override
    public Identifier getId()
    {
        return new Identifier(name);
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
        hash = 59 * hash + Objects.hashCode(this.name);
        hash = 59 * hash + Objects.hashCode(this.description);
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
        final Database other = (Database) obj;
        if (!Objects.equals(this.name, other.name))
        {
            return false;
        }
        if (!Objects.equals(this.description, other.description))
        {
            return false;
        }
        return true;
    }

}
