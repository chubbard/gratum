package gratum.source

import gratum.etl.Pipeline
import groovy.transform.CompileStatic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

@CompileStatic
class ClosureSource extends AbstractSource {

    Closure<Void> logic

    ClosureSource(Closure<Void> closure) {
        this.logic = closure
    }

    public static ClosureSource of(@DelegatesTo(Pipeline)
                                   @ClosureParams( value = SimpleType, options = ["gratum.etl.Pipeline"])
                                   Closure<Void> closure) {
        return new ClosureSource( closure )
    }

    @Override
    void doStart(Pipeline pipeline) {
        logic.delegate = pipeline
        logic.call(pipeline)
    }
}
