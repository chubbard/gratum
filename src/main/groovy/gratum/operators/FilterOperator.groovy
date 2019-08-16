package gratum.operators

import gratum.etl.Pipeline
import gratum.etl.RejectionCategory

class FilterOperator<T> implements Operator<T,T> {

    Closure<Boolean> callback

    FilterOperator(Closure<Boolean> callback) {
        this.callback = callback
    }

    /**
     * This adds a step to the Pipeline that passes all rows where the given closure returns true to the next step
     * on the pipeline.  All rows where the closure returns false are rejected.
     *
     * @param callback A callback that is passed a row, and returns a boolean.  All rows that return a false are rejected.
     * @return A Pipeline that contains only the rows that matched the filter.
     */
    @Override
    Pipeline<T> attach(Pipeline<T> source) {
        source.addStep("filter()") { T row ->
            return callback(row) ? row : source.reject("Row did not match the filter closure.", RejectionCategory.IGNORE_ROW )
        }
        return source
    }

}
