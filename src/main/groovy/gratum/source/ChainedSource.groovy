package gratum.source;

import gratum.etl.Pipeline;

public class ChainedSource implements Source {

    private Pipeline parent
    private Closure delegate

    ChainedSource(Pipeline parent) {
        this.parent = parent
    }

    @Override
    public void start(Closure closure) {
        this.delegate = closure
        parent.start()
    }

    public void process( Map row ) {
        this.delegate( row )
    }

    public void process( Collection<Map> rows ) {
        for( Map r : rows ) {
            this.delegate( r )
        }
    }
}
