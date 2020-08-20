package gratum.source

import gratum.etl.Pipeline

abstract class AbstractSource implements Source {

    String name

    @Override
    Pipeline into() {
        Pipeline pipeline = new Pipeline( name )
        pipeline.src = this
        return pipeline
    }
}
