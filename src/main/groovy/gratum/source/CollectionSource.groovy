package gratum.source

import gratum.etl.Pipeline

class CollectionSource implements Source {

    Collection<Map> source

    CollectionSource(Collection<Map> source) {
        this.source = source
    }

    @Override
    void start(Closure closure) {
        for( Map c : source ) {
            closure( c )
        }
    }

    public static Pipeline from( Map... src) {
        Pipeline pipeline = new Pipeline("Array(${src.size()})")
        pipeline.src = new CollectionSource( src.toList() )
        return pipeline
    }

    public static Pipeline from(Collection<Map> src ) {
        Pipeline pipeline = new Pipeline("Collection(${src.size()})")
        pipeline.src = new CollectionSource( src )
        return pipeline
    }
}
