package gratum.source

import gratum.etl.Pipeline
import groovy.transform.CompileStatic

@CompileStatic
class ClosureSource extends AbstractSource {

    Closure logic

    ClosureSource(Closure closure) {
        this.logic = closure
    }

    public static ClosureSource of(Closure<Void> closure) {
        return new ClosureSource( closure )
    }

    @Override
    void start(Pipeline pipeline) {
        logic.call(pipeline)
    }
}
