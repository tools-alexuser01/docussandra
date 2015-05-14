package com.strategicgains.docussandra.domain;

import com.strategicgains.docussandra.domain.abstractparent.Timestamped;

public class Metadata extends Timestamped
{
    private String id = "system";
    private String version;

    public Identifier getId()
    {
        return (hasId() ? new Identifier(id, getUpdatedAt()) : null);
    }

    public boolean hasId()
    {
        return (id != null);
    }

    public void id(String value)
    {
        this.id = value;
    }

    public String id()
    {
        return id;
    }

    public String version()
    {
        return version;
    }

    public void version(String version)
    {
        this.version = version;
    }
}
