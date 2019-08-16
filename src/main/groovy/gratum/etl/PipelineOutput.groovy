package gratum.etl

import gratum.operators.Operator

class PipelineOutput<T> implements Operator<T,T> {

    Closure<Void> performOutput

    List<Closure<OutputStream>> outputStreamChain = []

    OutputStream stream = null

    PipelineOutput(Closure<Void> onOutput) {
        this.performOutput = onOutput
    }

    public void append(Closure<OutputStream> aStream) {
        outputStreamChain << aStream
    }

    private OutputStream toStream() {
        if( !stream ) {
            outputStreamChain.reverseEach { Closure<OutputStream> callback ->
                stream = callback( stream )
            }
        }
        return stream
    }

    @Override
    Pipeline<T> attach(Pipeline<T> source) {

        source.addStep( "performOutput" ) { T t ->
            if( !outputStreamChain.isEmpty() ) new IllegalStateException("A stream was not defined for output.  You must use the append method.")
            performOutput(t, toStream())
            return t
        }

        return source
    }
}
