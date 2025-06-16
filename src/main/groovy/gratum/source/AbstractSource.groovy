package gratum.source

import gratum.etl.Pipeline
import groovy.transform.CompileStatic

@CompileStatic
abstract class AbstractSource implements Source {

    CharSequence name

    AbstractSource() {
    }

    AbstractSource(CharSequence name) {
        this.name = name
    }
/**
     * This converts the source into a Pipeline to attach steps to.
     * @return The Pipeline connected to this Source.
     */
    @Override
    Pipeline into() {
        return new Pipeline( name ).source( this )
    }

    /**
     * Overrides the name of the {@see gratum.etl.Pipeline}.
     * @param name A string with the name of the pipeline to use.
     * @return this Source
     */
    public <T extends AbstractSource> T name(CharSequence name) {
        this.name = name
        return (T)this
    }

    @Override
    void start(Pipeline pipeline) {
        try {
            doStart( pipeline )
        } finally {
            pipeline.finished()
        }
    }

    abstract void doStart(Pipeline pipeline)

    void setName(CharSequence name) {
        this.name = name
    }
}
