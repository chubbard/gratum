package gratum.source

import gratum.etl.Pipeline

class SingleSource<T> implements Source  {

    T startObject

    public static <V> Pipeline<V> of( V initialObject, String name = "unknown" ) {
        Pipeline<V> pipeline = new Pipeline<>(name)
        pipeline.src = new SingleSource<V>( initialObject )
        return pipeline
    }

    SingleSource(T startObject) {
        this.startObject = startObject
    }

    @Override
    void start(Closure closure) {
        closure( startObject )
    }
}
