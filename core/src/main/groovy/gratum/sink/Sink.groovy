package gratum.sink

import gratum.etl.Pipeline

interface Sink<T> extends Closeable, AutoCloseable {

    String getName()

    void attach(Pipeline pipeline )

    Map<String,Object> getResult()

}