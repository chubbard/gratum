package gratum.operators

import gratum.etl.Pipeline

class SetFieldOperator implements Operator<Map,Map> {

    String fieldName
    Object value

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
