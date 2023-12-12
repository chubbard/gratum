package gratum.sink

import gratum.etl.Pipeline
import gratum.source.Source

interface Sink<T> extends Closeable, AutoCloseable {

    String getName()

    void attach(Pipeline pipeline )

    Source getResult()

}