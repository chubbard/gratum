package gratum.operators

import gratum.etl.Pipeline
import gratum.etl.RejectionCategory

class DataTypesOperator<T> implements Operator<Map,Map> {

    private String name
    private String column
    private Closure conversion

    DataTypesOperator(String name, String column, Closure<T> conversion) {
        this.name = name
        this.column = column
        this.conversion = conversion
    }

    public static <T> Operator convert( String name, String fieldName, Closure<T> conversion ) {
        return new DataTypesOperator( name, fieldName, conversion )
    }

    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {
        source.addStep("${name}(${column})") { Map row ->
            String value = row[column] as String
            try {
                if (value) row[column] = conversion.call( value )
                return row
            } catch( NumberFormatException ex) {
                source.reject("Could not parse ${value} as a ${name}", RejectionCategory.INVALID_FORMAT)
                return null
            }
        }
        return source
    }
}
