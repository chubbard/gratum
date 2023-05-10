package gratum.dsl

import gratum.etl.Pipeline
import gratum.sink.Sink
import gratum.source.ClosureSource
import gratum.source.CollectionSource
import gratum.source.CsvSource
import gratum.source.FileSystemSource
import gratum.source.HttpSource
import gratum.source.JdbcSource
import gratum.source.JsonSource
import gratum.source.Source
import gratum.source.XlsxSource
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import groovyx.net.http.HttpBuilder

class PipelineDsl {

    protected @Delegate(excludes = ["source"]) Pipeline pipeline

    void from(Iterable<Map<String,Object>> src, @DelegatesTo(CollectionSource) Closure configure ) {
        source( CollectionSource.of(src), configure )
    }

    void from(@DelegatesTo(Pipeline.class)
              @ClosureParams( value = FromString.class, options = ["gratum.etl.Pipeline"])
                      Closure<Void> logic) {
        source( ClosureSource.of(logic) )
    }

    void csv(final File file, @DelegatesTo(CsvSource) Closure closure ) {
        source( CsvSource.of(file), closure )
    }

    void json(final File file, @DelegatesTo(JsonSource) Closure closure ) {
        source( JsonSource.json( file ), closure )
    }

    void files(Iterable<File> files, @DelegatesTo(FileSystemSource) Closure configuration) {
        source( FileSystemSource.files(files), configuration )
    }

    void http(String url, HttpBuilder httpBuilder = null ) {
        source( HttpSource.http(url, httpBuilder) )
    }

    void http(Closure configuration) {
        source( HttpSource.http(configuration) )
    }

    void https(Closure configuration) {
        source( HttpSource.https(configuration) )
    }

    void https(String url, HttpBuilder httpBuilder = null ) {
        source( HttpSource.https(url, httpBuilder) )
    }

    void xlsx(File file,
              @DelegatesTo(XlsxSource.class) Closure configuration ) {
        source( XlsxSource.xlsx(file), configuration )
    }

    void database(String dbUrl, String username, String password, @DelegatesTo(JdbcSource.class) Closure configuration ) {
        source( JdbcSource.database(dbUrl, username, password), configuration )
    }

    public <T extends Source> void source( final T source, @DelegatesTo(value = Source, genericTypeIndex = 0) Closure configure = null ) {
        configure?.resolveStrategy = Closure.DELEGATE_ONLY
        configure?.delegate = source
        configure?.call()
        pipeline = source.into()
    }

    void step( String name,
               @DelegatesTo(Pipeline)
               @ClosureParams( value = FromString.class, options = ["java.util.Map<String,Object>"])
               Closure<Map<String,Object>> closure) {
        pipeline.addStep( name, closure)
    }

    void addField(String filename,
                  @DelegatesTo(Pipeline)
                  @ClosureParams( value = FromString.class, options = ["java.util.Map<String,Object>"])
                  Closure<Object> closure) {
        pipeline.addField( filename, closure )
    }

    void renameFields(Map<String,String> fieldNames,
                      @DelegatesTo(Pipeline)
                      @ClosureParams( value = FromString.class, options = ["java.util.Map<String,Object>"])
                      Closure closure) {
        pipeline.renameFields(fieldNames )
    }

    void sink(Sink<Map<String,Object>> sink ) {
        pipeline.save( sink )
    }

    def <T> T withPlugin(Class<T> plugin) {
        return pipeline as T
    }
}
