package gratum.source

import gratum.etl.Pipeline

class ClosureSource extends AbstractSource {

    Closure closure

    ClosureSource(Closure closure) {
        this.closure = closure
    }

    @Override
    void start(Pipeline pipeline) {
        this.closure(pipeline)
    }
}
