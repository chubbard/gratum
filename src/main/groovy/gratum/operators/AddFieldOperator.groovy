package gratum.operators

import gratum.etl.Pipeline
import gratum.etl.Rejection

class AddFieldOperator implements Operator {

    String fieldName
    Closure<Map<String,Object>> fieldValue

    public static addField( String fieldName, Closure<Map<String,Object>> fieldValue ) {
        return new AddFieldOperator( fieldName, fieldValue )
    }

    AddFieldOperator(String fieldName, Closure fieldValue) {
        this.fieldName = fieldName
        this.fieldValue = fieldValue
    }

    @Override
    Pipeline attach(Pipeline source) {
        source.addStep("addField(${fieldName})") { Map row ->
            Object value = fieldValue(row)
            if( value instanceof Rejection ) return value
            row[fieldName] = value
            return row
        }
        return source
    }
}
