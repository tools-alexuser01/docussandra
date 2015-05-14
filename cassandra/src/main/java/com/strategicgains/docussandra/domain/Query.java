package com.strategicgains.docussandra.domain;

import java.util.List;


public class Query
{

    private static int DEFAULT_LIMIT = 100;

    //	public Map<String, String> variables;
//	@Required("Table name")
    private String table;

//	@Required("Columns")
    private List<String> columns;

    private String where;
    private int limit = DEFAULT_LIMIT;
    public int offset;

    public Query()
    {
    }

    /**
     * @return the DEFAULT_LIMIT
     */
    public static int getDEFAULT_LIMIT()
    {
        return DEFAULT_LIMIT;
    }

    /**
     * @param aDEFAULT_LIMIT the DEFAULT_LIMIT to set
     */
    public static void setDEFAULT_LIMIT(int aDEFAULT_LIMIT)
    {
        DEFAULT_LIMIT = aDEFAULT_LIMIT;
    }

    /**
     * @return the table
     */
    public String getTable()
    {
        return table;
    }

    /**
     * @param table the table to set
     */
    public void setTable(String table)
    {
        this.table = table;
    }

    /**
     * @return the columns
     */
    public List<String> getColumns()
    {
        return columns;
    }

    /**
     * @param columns the columns to set
     */
    public void setColumns(List<String> columns)
    {
        this.columns = columns;
    }

    /**
     * @return the where
     */
    public String getWhere()
    {
        return where;
    }

    /**
     * @param where the where to set
     */
    public void setWhere(String where)
    {
        this.where = where;
    }

    /**
     * @return the limit
     */
    public int getLimit()
    {
        return limit;
    }

    /**
     * @param limit the limit to set
     */
    public void setLimit(int limit)
    {
        this.limit = limit;
    }

    @Override
    public String toString()
    {
        return "Query{" + "table=" + table + ", columns=" + columns + ", where=" + where + ", limit=" + limit + ", offset=" + offset + '}';
    }

    
}
