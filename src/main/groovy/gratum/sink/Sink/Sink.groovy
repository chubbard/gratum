package gratum.sink.Sink

import gratum.etl.Pipeline

interface Sink<T> extends Closeable {

    String getName()

    void attach(Pipeline pipeline )

    Map<String,Object> getResult()

}