package gratum.operators

import gratum.etl.Pipeline
import gratum.etl.Rejection
import gratum.source.ChainedSource

class InjectOperator<T> implements Operator<T,T> {
    private String name
    private Closure<Collection<T>> inject

    InjectOperator(String name, Closure<Collection<T>> inject) {
        this.name = name
        this.inject = inject
    }

    @Override
    Pipeline attach(Pipeline source) {
        Pipeline next = new Pipeline(name)
        next.src = new ChainedSource( source )

        source.addStep(name) { T row ->
            def result = inject( row )
            if( !result ) {
                next.lastRejection = delegate.lastRejection
                next.doRejections( row, name, -1)
                return row
            } else {
                Collection<T> cc = result
                ((ChainedSource)next.src).process( cc )
            }
            return row
        }
        next.copyStatistics( source )
        return next
    }
}
