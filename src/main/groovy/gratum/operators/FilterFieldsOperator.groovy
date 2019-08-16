package gratum.operators

import gratum.etl.Pipeline
import gratum.etl.RejectionCategory

class FilterFieldsOperator implements Operator<Map,Map> {

    private Map<String,Object> fields

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
