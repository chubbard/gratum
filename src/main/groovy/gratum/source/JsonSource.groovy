package gratum.source

import gratum.etl.Pipeline
import groovy.json.JsonSlurper

class JsonSource extends AbstractSource {

    Reader reader
    List<String> jsonPath = []
    def rootJson

    JsonSource(reader ) {
        this.reader = reader
    }

    public JsonSource path(List<String> aPath) {
        this.jsonPath = aPath
        return this
    }

    public static JsonSource json(File file ) {
        return json( new FileReader( file ) )
    }

    public static JsonSource json(Reader reader ) {
        return new JsonSource( reader )
    }

    public static JsonSource json(String json ) {
        return new JsonSource( new StringReader(json) )
    }

    @Override
    void start(Pipeline pipeline) {
        try {
            parseJson(reader, pipeline)
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
}
