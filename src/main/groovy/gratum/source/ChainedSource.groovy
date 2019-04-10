package gratum.source;

import gratum.etl.Pipeline;

public class ChainedSource implements Source {

    private Pipeline parent
    private Pipeline delegate
    int line = 1

    ChainedSource(Pipeline parent) {
        this.parent = parent
    }

    @Override
    void start(Pipeline pipeline) {
        this.delegate = pipeline
        parent.start()
    }

    public void process( Map row ) {
        this.delegate.process( row, line++ )
    }

    public void process( Collection<Map> rows ) {
        for( Map r : rows ) {
            this.delegate.process( r, line++ )
        }
    }
}
