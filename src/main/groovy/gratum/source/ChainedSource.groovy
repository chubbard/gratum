package gratum.source;

import gratum.etl.Pipeline
import groovy.transform.CompileStatic;

@CompileStatic
public class ChainedSource<T> implements Source {

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

    public void process( T row ) {
        this.delegate.call( row )
    }

    public void process( Collection<T> rows ) {
        for( T r : rows ) {
            process( r )
        }
    }
}
