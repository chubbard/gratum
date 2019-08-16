package gratum.operators

import gratum.etl.Pipeline

class SetFieldOperator implements Operator<Map,Map> {

    String fieldName
    Object value

    /**
     * Sets a fieldName in each row to the given value.
     * @param fieldName The new field name to add
     * @param value the value of the new field name
     * @return An operator where each row has a fieldname set to the given value
     */
    public static set( String fieldName, Object value ) {
        return new SetFieldOperator( fieldName, value );
    }

    SetFieldOperator(String fieldName, Object value) {
        this.fieldName = fieldName
        this.value = value
    }

    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {

        source.addStep("setField(${fieldName})") { Map row ->
            row[fieldName] = value
            return row
        }

        return source
    }
}
