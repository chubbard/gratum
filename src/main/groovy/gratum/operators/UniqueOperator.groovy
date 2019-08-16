package gratum.operators

import gratum.etl.Pipeline
import gratum.etl.RejectionCategory

class UniqueOperator implements Operator<Map,Map> {

    private String column

    UniqueOperator(String column) {
        this.column = column
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
