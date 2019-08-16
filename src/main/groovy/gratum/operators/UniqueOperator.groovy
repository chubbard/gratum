package gratum.operators

import gratum.etl.Pipeline
import gratum.etl.RejectionCategory

class UniqueOperator implements Operator<Map,Map> {

    private String column

    UniqueOperator(String column) {
        this.column = column
    }

    /**
     * Only allows rows that are unique per the given column.
     *
     * @param column The column name to use for checking uniqueness
     * @return A Pipeline that only contains the unique rows for the given column
     */
    public static Operator unique(String column) {
        return new UniqueOperator( column )
    }

    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {
        Set<Object> unique = [:] as HashSet
        source.addStep("unique(${column})") { Map row ->
            if( unique.contains(row[column]) ) return source.reject("Non-unique row returned", RejectionCategory.IGNORE_ROW)
            unique.add( row[column] )
            return row
        }
        return source
    }
}
