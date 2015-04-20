package com.strategicgains.docussandra.domain;

import com.mongodb.util.JSON;
import java.util.UUID;

import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.domain.UuidIdentifiable;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.Required;
import java.util.Objects;

public class Document
        extends AbstractTimestampedIdentifiable
        implements UuidIdentifiable {
    //TODO: allow something other than UUID as object id.
    //TODO: add any necessary metadata regarding a document.
    //TODO: documents are versioned per transaction via updateAt timestamp.

    private UUID id;

    // need a separate version (as opposed to updatedAt)?
//	private long version;
    @Required("Table")
    @ChildValidation
    private TableReference table;

    // The JSON document.
    private String object;

    public Document() {
    }

    @Override
    public Identifier getId() {
        return new Identifier(databaseName(), tableName(), id, getUpdatedAt());
    }

    @Override
    public void setId(Identifier id) {
        // Do nothing. Throw.
        throw new UnsupportedOperationException("This is not a valid call for this object.");
    }

    @Override
    public UUID getUuid() {
        return id;
    }

    @Override
    public void setUuid(UUID id) {
        this.id = id;
    }
    
    public boolean hasTable() {
        return (table != null);
    }

    public Table table() {
        return (hasTable() ? table.asObject() : null);
    }

    public void table(String database, String table) {
        this.table = new TableReference(database, table);
    }

    public void table(Table table) {
        this.table = (table != null ? new TableReference(table) : null);
    }

    public String tableName() {
        return (hasTable() ? table.name() : null);
    }

    public String databaseName() {
        return (hasTable() ? table.database() : null);
    }

    public String object() {
        return object;
    }

    public void object(String json) {
        this.object = json;
    }

    @Override
    public String toString() {
        return "Document{" + "id=" + id + ", table=" + table + ", object=" + object + '}';
    }

    @Override
    public int hashCode()
    {
        int hash = 7;
        hash = 29 * hash + Objects.hashCode(this.id);
        hash = 29 * hash + Objects.hashCode(this.table);
        hash = 29 * hash + Objects.hashCode(this.object);
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
        final Document other = (Document) obj;
        if (!Objects.equals(this.id, other.id))
        {
            return false;
        }
        if (!Objects.equals(this.table, other.table))
        {
            return false;
        }
        if (!Objects.equals(JSON.parse(this.object), JSON.parse(other.object)))
        {
            return false;
        }
        return true;
    }
    
    

}
