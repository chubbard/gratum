package gratum.operators

import gratum.etl.Pipeline

class TrimOperator implements Operator<Map,Map> {

    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {
        source.addStep("trim()") { Map<String,Object> row ->
            row.each { String key, Object value -> row[key] = (value as String).trim() }
            return row
        }
        return source
    }
}
