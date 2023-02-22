package gratum.source

import gratum.etl.Pipeline
import groovy.transform.CompileStatic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString

@CompileStatic
class ClosureSource extends AbstractSource {

    Closure logic

    ClosureSource(Closure closure) {
        this.logic = closure
    }

    public static ClosureSource of(@DelegatesTo(Pipeline)
//                                   @ClosureParams( value = FromString, options = ["gratum.etl.Pipeline"])
                                   Closure<Void> closure) {
        return new ClosureSource( closure )
    }

    @Override
    void doStart(Pipeline pipeline) {
        logic.delegate = pipeline
        logic.call(pipeline)
    }
}
