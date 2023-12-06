package gratum.sink

import gratum.etl.Pipeline
import groovy.transform.CompileStatic

@CompileStatic
class CollectionSink implements Sink {

    final String destColumn
    final Collection dest

    CollectionSink(String destColumn, Collection dest = []) {
        this.destColumn = destColumn
        this.dest = dest
    }

    @Override
    String getName() {
        return "Collection(${destColumn})"
    }

    @Override
    void attach(Pipeline pipeline) {
        pipeline.addStep("sink -> collection") { row ->
            dest.add( row )
            row
        }
    }

    @Override
    Map<String, Object> getResult() {
        return [
                (destColumn): dest
        ] as Map<String,Object>
    }

    @Override
    void close() throws IOException {
    }
}
