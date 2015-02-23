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

    private final String boundStatementSyntax = "";

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
                    break;
                case OPERATOR:
                    boundStatementBuilder.append(token);
                    break;
                case VALUE:
                    boundStatementBuilder.append(token);
                    break;
                case CONJUNCTION:
                    
                    break;
                case END:

                    break;
            }
            boundStatementBuilder.append(" ");//tokens need to be re-space seperated
        }

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

}
