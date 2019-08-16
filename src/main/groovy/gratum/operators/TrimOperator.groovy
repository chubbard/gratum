package gratum.operators

import gratum.etl.Pipeline

class TrimOperator implements Operator<Map,Map> {

    /**
     * Returns a Pipeline where all white space is removed from all columns contained within the rows.
     *
     * @return Operator where all rows has white space removed.
     */
    public static Operator<Map,Map> trim() {
        return new TrimOperator()
    }


    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {
        source.addStep("trim()") { Map<String,Object> row ->
            row.each { String key, Object value -> row[key] = (value as String).trim() }
            return row
        }
        return source
    }
}
