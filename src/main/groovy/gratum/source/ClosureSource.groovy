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

    public static ClosureSource of(@ClosureParams( value = FromString, options = ["gratum.etl.Pipeline"]) Closure<Void> closure) {
        return new ClosureSource( closure )
    }

    @Override
    void doStart(Pipeline pipeline) {
        logic.call(pipeline)
    }
}
