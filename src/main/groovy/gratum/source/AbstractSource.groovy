package gratum.source

import gratum.etl.Pipeline

abstract class AbstractSource implements Source {

    String name

    /**
     * This converts the source into a Pipeline to attach steps to.
     * @return The Pipeline connected to this Source.
     */
    @Override
    Pipeline into() {
        Pipeline pipeline = new Pipeline( name )
        pipeline.src = this
        return pipeline
    }

    /**
     * Overrides the name of the {@see gratum.etl.Pipeline}.
     * @param name A string with the name of the pipeline to use.
     * @return this Source
     */
    public <T extends AbstractSource> T name(String name) {
        this.name = name
        return (T)this
    }
}
