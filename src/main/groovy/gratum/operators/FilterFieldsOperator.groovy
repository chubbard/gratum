package gratum.operators

import gratum.etl.Pipeline
import gratum.etl.RejectionCategory

class FilterFieldsOperator implements Operator<Map,Map> {

    private Map<String,Object> fields

    /**
     * This adds a step tot he Pipeline that passes all rows where the values of the columns on the given Map are equal
     * to the columns in the row.  This is a boolean AND between columns.  For example:
     *
     * .filter( [ hair: 'Brown', eyeColor: 'Blue' ] )
     *
     * In this example all rows where hair = Brown AND eyeColor = Blue are passed through the filter.
     *
     * @param columns a Map that contains the columns, and their values that are passed through
     * @return
     */
    public static Operator<Map,Map> filerFields( Map<String,Object> fields ) {
        return new FilterFieldsOperator( fields )
    }

    public FilterFieldsOperator(Map<String, Object> fields) {
        this.fields = fields
    }

    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {
        source.addStep( "filter ${ nameOf(fields) }" ) { Map row ->
            if(matches(fields, row)) {
                return row
            } else {
                return source.reject("Row did not match the filter ${fields}", RejectionCategory.IGNORE_ROW )
            }
        }
    }

    private boolean matches(Map columns, Map row) {
        return columns.keySet().inject(true) { match, key ->
            if( columns[key] instanceof Collection ) {
                match && ((Collection)columns[key]).contains( row[key] )
            } else {
                match && row[key] == columns[key]
            }
        }
    }

    private String nameOf(Map columns) {
        return columns.keySet().collect() { key -> key + "->" + columns[key] }.join(',')
    }

}
