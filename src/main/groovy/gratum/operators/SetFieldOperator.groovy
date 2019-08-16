package gratum.operators

import gratum.etl.Pipeline

class SetFieldOperator implements Operator<Map<String,Object>,Map<String,Object>> {

    String fieldName
    Object value

    SetFieldOperator(String fieldName, Object value) {
        this.fieldName = fieldName
        this.value = value
    }

    @Override
    Pipeline<Map<String,Object>> attach(Pipeline<Map<String,Object>> source) {

        source.addStep("setField(${fieldName})") { Map row ->
            row[fieldName] = value
            return row
        }

        return source
    }
}
