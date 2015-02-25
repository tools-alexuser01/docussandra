package com.strategicgains.docussandra.domain;

import java.util.ArrayList;
import java.util.StringTokenizer;
import org.restexpress.common.util.StringUtils;

/**
 * Object that represents a CQL where clause to the extent that we need to
 * process it.
 *
 * @author udeyoje
 */
public class WhereClause {

    private static final int FIELD = 0;
    private static final int OPERATOR = 1;
    private static final int VALUE = 2;
    private static final int CONJUNCTION = 3;
    private static final int END = 4;

    private final String whereClause;

    private String boundStatementSyntax = "";

    /**
     * Field Names in a where clause. Corresponds to values.
     */
    private ArrayList<String> fields = new ArrayList<>();
    /**
     * Field Values in a where clause. Corresponds to fields.
     */
    private ArrayList<String> values = new ArrayList<>();

    public WhereClause(String whereClause) {
        this.whereClause = whereClause;
        parse(whereClause);
    }

    //TODO: this will need improvment; handle exceptions
    private void parse(String whereClause) {

        StringBuilder boundStatementBuilder = new StringBuilder();
        //foo = 'fooish' AND bar < 'barish'... [ORDER BY] foo
        String[] tokens = whereClause.split("\\Q \\E");
        int currentOperator = FIELD;
        for (String token : tokens) {
            switch (currentOperator) {
                case FIELD:
                    fields.add(token);
                    boundStatementBuilder.append(token);
                    currentOperator = OPERATOR;
                    break;
                case OPERATOR:
                    boundStatementBuilder.append(token);
                    currentOperator = VALUE;
                    break;
                case VALUE:
                    boundStatementBuilder.append("?");
                    values.add(token.replaceAll("\\Q'\\E", ""));
                    currentOperator = CONJUNCTION;
                    break;
                case CONJUNCTION:
                    boundStatementBuilder.append(token);
                    currentOperator = FIELD;
                    //TODO: we could have CQL injection here; it pretty much just appends the end of the statement; should check specifically
                    break;
                default:// this should not run; defaults to conjunction
                    boundStatementBuilder.append(token);
                    break;
            }
            boundStatementBuilder.append(" ");//tokens need to be re-space seperated
        }
        boundStatementSyntax = boundStatementBuilder.toString().trim();

    }

    public String getBoundStatementSyntax() {
        return boundStatementSyntax;
    }

    /**
     * Field Names in a where clause. Corresponds to values.
     *
     * @return the fields
     */
    public ArrayList<String> getFields() {
        return fields;
    }

    /**
     * Field Values in a where clause. Corresponds to fields.
     *
     * @return the values
     */
    public ArrayList<String> getValues() {
        return values;
    }

    /**
     * @return the original where clause passed in. Use as a reference only.
     */
    public String getWhereClause() {
        return whereClause;
    }

    @Override
    public String toString() {
        return "WhereClause{" + "whereClause=" + whereClause + ", boundStatementSyntax=" + boundStatementSyntax + ", fields=" + fields + ", values=" + values + '}';
    }
    
    

}
