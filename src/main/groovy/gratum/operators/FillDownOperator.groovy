package gratum.operators

import gratum.etl.Pipeline

class FillDownOperator implements Operator<Map,Map> {

    private Closure<Boolean> decider

    public FillDownOperator(Closure<Boolean> decider) {
        this.decider = decider
    }

    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {

        Map previousRow = null

        source.addStep("fillDownBy()") { Map<String,Object> row ->
            if( previousRow && decider( row, previousRow ) ) {
                row.each { String col, Object value ->
                    // todo refactor valid_to out for excluded
                    if (col != "valid_To" && (value == null || value.toString().isEmpty())) {
                        row[col] = previousRow[col]
                    }
                }
            }
            previousRow = (Map)row.clone()
            return row
        }

        return source

    }

}
