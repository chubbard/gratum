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

    /**
     * Returns a Pipeline where the given column is coverted from a string to a java.lang.Double.
     * @param column The name of the column to convert into a Double
     * @return An Operator where all rows contains a java.lang.Double at the given column
     */
    public static Operator<Map,Map> asDouble(String fieldName) {
        return convert( "Double", fieldName ) { String value ->
            Double.parseDouble(value)
        }
    }

    /**
     * Parses the string value at given fieldname into a java.lang.Integer value.
     * @param column containing a string to be turned into a java.lang.Integer
     * @return An Operator where all rows contain a java.lang.Integer at given column
     */
    public static Operator<Map,Map> asInt(String fieldName) {
        return convert( "Integer", fieldName ) { String value ->
            Integer.parseInt(value)
        }
    }

    /**
     * Parses the string value at given fieldname into a java.lang.Boolean value.  It understands values like: Y/N, YES/NO, TRUE/FALSE, 1/0, T/F.
     * @param column containing a string to be turned into a java.lang.Boolean
     * @return An Operator where all rows contain a java.lang.Boolean at given column
     */
    public static Operator<Map,Map> asBoolean(String fieldName) {
        return convert( "Boolean", fieldName ) { String value ->
            if( value ) {
                switch( value ) {
                    case "Y":
                    case "y":
                    case "yes":
                    case "YES":
                    case "Yes":
                    case "1":
                    case "T":
                    case "t":
                        return true
                    case "n":
                    case "N":
                    case "NO":
                    case "no":
                    case "No":
                    case "0":
                    case "F":
                    case "f":
                    case "null":
                    case "Null":
                    case "NULL":
                    case null:
                        return false
                    default:
                        return Boolean.parseBoolean(value)
                }
            }
            return null
        }
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
