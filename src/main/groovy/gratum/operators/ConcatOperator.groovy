package gratum.operators

import gratum.etl.Pipeline

/**
 * Concatentates the rows from this pipeline and the given pipeline.  The resulting Pipeline will process all
 * rows from this pipeline and the src pipeline.
 */
class ConcatOperator<T> implements Operator<T,T> {

    Pipeline<T> source

    ConcatOperator(Pipeline<T> source) {
        this.source = source
    }

    /**
     * Concatentates the rows from this pipeline and the given pipeline.  The resulting Pipeline will process all
     * rows from this pipeline and the src pipeline.
     *
     * @param source The pipeline
     * @return Returns a new pipeline that combines all of the rows from this pipeline and the src pipeline.
     */
    public static Operator<T,T> concat(Pipeline<T> source ) {
        return new ConcatOperator(source)
    }

    @Override
    Pipeline<T> attach(Pipeline<T> next) {
        next.after {
            int line = 0
            source.addStep("concat(${source.name})") { T row ->
                line++
                next.process( row, line )
                return row
            }.start()
        }
        return next
    }
}
