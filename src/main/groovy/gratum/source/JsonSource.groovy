package gratum.source

import gratum.etl.Pipeline
import groovy.json.JsonSlurper

class JsonSource extends AbstractSource {

    Reader reader
    List<String> jsonPath = []
    def rootJson
    boolean recordPerLine = false

    JsonSource(String name, Reader reader ) {
        super(name)
        this.reader = reader
    }

    public JsonSource path(List<String> aPath) {
        this.jsonPath = aPath
        return this
    }

    public static JsonSource json(File file ) {
        return json( new FileReader( file ), file.name )
    }

    public static JsonSource json(Reader reader, String name = "Reader") {
        return new JsonSource( name, reader )
    }

    public static JsonSource json(String json, String name = "String") {
        return new JsonSource( name, new StringReader(json) )
    }

    public JsonSource recordPerLine( boolean recordPerLine ) {
        this.recordPerLine = recordPerLine
        return this
    }

    @Override
    void start(Pipeline pipeline) {
        try {
            if( recordPerLine ) {
                parseRecordPerLine( reader, pipeline )
            } else {
                parseJson(reader, pipeline)
            }
        } catch(e) {
            throw(e)
        } finally {
            rootJson = null
            reader.close()
        }
    }

    private void parseJson(Reader reader, Pipeline pipeline) {
        rootJson = JsonSlurper.newInstance().parse( reader )
        recurseJson(rootJson, jsonPath, pipeline)
    }

    private int recurseJson(def json, List<String> path, Pipeline callback, int lines = 0) {
        if( json instanceof Collection ) {
            for(int i = 0; i < json.size(); i++){
                if(lines++ == recurseJson(json[i], path.size() > 0 ? path.subList(1, path.size()) : path, callback, lines)){
                    return lines
                }
            }
            return lines
        } else if( path.isEmpty() ) {
            json["_root_json"] = rootJson // insert the rootJson as a special field in case we want to read it
            return callback.process(json, lines) ? ++lines : lines
        } else {
            json = json[ path.first() ]
            return recurseJson(json, path.subList(1, path.size()), callback, lines)
        }
    }

    void parseRecordPerLine(Reader reader, Pipeline pipeline) {
        JsonSlurper json = JsonSlurper.newInstance()
        reader.eachLine { line ->
            String trimmed = line.trim()
            if( trimmed ) {
                rootJson = json.parseText( trimmed )
                recurseJson( rootJson, jsonPath, pipeline )
            }
        }
    }
}
