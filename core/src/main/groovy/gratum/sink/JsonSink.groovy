package gratum.sink

import gratum.etl.FileOpenable
import gratum.etl.Pipeline
import gratum.source.CollectionSource
import gratum.source.Source
import groovy.json.JsonOutput
import groovy.transform.CompileStatic

@CompileStatic
class JsonSink implements Sink<Map<String,Object>> {

    String name
    BufferedWriter writer
    Collection<String> columns
    File output

    JsonSink(File file, Collection<String> columns = null){
        this.output = file
        this.name = file.name
        this.writer = file.newWriter("UTF-8")
        this.columns = columns
    }

    JsonSink(String name, Writer writer, Collection<String> columns = null) {
        this.name = name
        this.writer = new BufferedWriter( writer )
        this.columns = columns
    }

    @Override
    String getName() {
        name
    }

    @Override
    void attach(Pipeline pipeline) {
        writer.writeLine("[")
        if( columns ) {
            pipeline.addStep("Json to ${name}") { Map row ->
                String json = JsonOutput.toJson( row.subMap( columns ) )
                writer.write( json )
                writer.write(",\n")
                return row
            }
        } else {
            pipeline.addStep("Json to ${name}") { Map row ->
                String json = JsonOutput.toJson( row )
                writer.write( json )
                writer.write(",\n")
                return row
            }
        }
    }

    @Override
    Source getResult() {
        if( output ) {
            return CollectionSource.of( [file: output, filename: output.absolutePath, stream: new FileOpenable(output)] )
        } else {
            return CollectionSource.of( [name: name] )
        }
    }

    @Override
    void close() throws IOException {
        writer.write("\n]")
        writer.flush()
        writer.close()
    }
}