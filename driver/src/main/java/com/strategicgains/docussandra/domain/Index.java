package com.strategicgains.docussandra.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import com.strategicgains.docussandra.Constants;
import com.strategicgains.repoexpress.domain.AbstractTimestampedIdentifiable;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.syntaxe.Validatable;
import com.strategicgains.syntaxe.ValidationException;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;
import java.util.Objects;

public class Index
        extends AbstractTimestampedIdentifiable
        implements Validatable {

    @Required("Table")
    @ChildValidation
    private TableReference table;

    @RegexValidation(name = "Index name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
    private String name;

    private boolean isUnique = false;

    /**
     * This is how many items will be stored in a single wide row, before
     * creating another wide row.
     */
    //TODO: refactor to new concept of infinately sized buckets, but with a limit number of buckets
    private long bucketSize = 2000l;

    /**
     * The list of fields, in order, that are being indexed. Prefixing a field
     * with a dash ('-') means it's order in descending order.
     */
    @Required("Fields")
    private List<String> fields;

//	@Required("Index Type")
//	private IndexType type;
    /**
     * Consider the index is only concerned with only a partial dataset. In this
     * case, instead of storing the entire BSON payload, we store only a
     * subset--those listed in includeOnly.
     */
    private List<String> includeOnly;
    
    /**
     * Field indicating if this index should be presently considered active. TODO: Store this in the DB.
     */
    private boolean isActive;

    public Index() {
    }

    public Index(String name) {
        this();
        name(name);
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

    public String name() {
        return name;
    }

    public Index name(String name) {
        this.name = name;
        return this;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public Index isUnique(boolean value) {
        this.isUnique = value;
        return this;
    }

    public void fields(List<String> props) {
        this.fields = new ArrayList<String>(props);
    }

    public List<String> fields() {
        return (fields == null ? Collections.<String>emptyList() : Collections.unmodifiableList(fields));
    }

    public void includeOnly(List<String> props) {
        if(props != null && !props.isEmpty()){
            this.includeOnly = new ArrayList<String>(props);
        }
    }

    public List<String> includeOnly() {
        return (includeOnly == null ? Collections.<String>emptyList() : Collections.unmodifiableList(includeOnly));
    }

    @Override
    public Identifier getId() {
        return new Identifier(databaseName(), tableName(), name);
    }

    @Override
    public void setId(Identifier id) {
        // intentionally left blank.
    }

    public long bucketSize() {
        return bucketSize;
    }

    public void bucketSize(long bucketSize) {
        this.bucketSize = bucketSize;
    }

    public void iterateFields(Callback<IndexField> callback) {
        for (String field : fields) {
            callback.process(new IndexField(field));
        }
    }

    /**
     * Field indicating if this index should be presently considered active. TODO: Store this in the DB.
     * @return the isActive
     */
    public boolean isIsActive()
    {
        return isActive;
    }

    /**
     * Field indicating if this index should be presently considered active. TODO: Store this in the DB.
     * @param isActive the isActive to set
     */
    public void setIsActive(boolean isActive)
    {
        this.isActive = isActive;
    }

    public class IndexField {

        private String field;
        private boolean isAscending = true;

        public IndexField(String value) {
            field = value.trim();

            if (field.trim().startsWith("-")) {
                field = value.substring(1);
                isAscending = false;
            }
        }

        public String field() {
            return field;
        }

        public boolean isAscending() {
            return isAscending;
        }
    }

    @Override
    public void validate() {
        final List<String> errors = new ArrayList<String>();

        if (fields.isEmpty()) {
            errors.add("Fields is required.");
        }

        Pattern fieldPattern = Pattern.compile("^[\\+-]?\\w+");

        for (String field : fields) {
            if (!fieldPattern.matcher(field).matches()) {
                errors.add("Invalid index field name: " + field);
            }
        }

        Pattern includePattern = Pattern.compile("^\\w+");

        if (includeOnly != null) {
            if (includeOnly.isEmpty()) {
                errors.add("'includeOnly' cannot be empty, if included.");
            }

            for (String field : includeOnly) {
                if (!includePattern.matcher(field).matches()) {
                    errors.add("Invalid 'includeOnly' field name: " + field);
                }
            }
        }

        if (!errors.isEmpty()) {
            throw new ValidationException(errors);
        }
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 83 * hash + Objects.hashCode(this.table);
        hash = 83 * hash + Objects.hashCode(this.name);
        hash = 83 * hash + (this.isUnique ? 1 : 0);
        hash = 83 * hash + (int) (this.bucketSize ^ (this.bucketSize >>> 32));
        hash = 83 * hash + Objects.hashCode(this.fields);
        hash = 83 * hash + Objects.hashCode(this.includeOnly);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Index other = (Index) obj;
        if (!Objects.equals(this.table, other.table)) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (this.isUnique != other.isUnique) {
            return false;
        }
        if (this.bucketSize != other.bucketSize) {
            return false;
        }
        if (!Objects.equals(this.fields, other.fields)) {
            return false;
        }
        if (!Objects.equals(this.includeOnly, other.includeOnly)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Index{" + "table=" + table + ", name=" + name + ", isUnique=" + isUnique + ", bucketSize=" + bucketSize + ", fields=" + fields + ", includeOnly=" + includeOnly + '}';
    }

    
}
