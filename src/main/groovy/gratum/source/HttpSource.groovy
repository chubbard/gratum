package gratum.source

import groovy.transform.CompileStatic
import groovyx.net.http.HttpBuilder
import groovyx.net.http.HttpConfig
import groovyx.net.http.HttpObjectConfig

import static groovyx.net.http.HttpBuilder.configure

import gratum.etl.Pipeline

/**
 * @deprecated This class will be removed in the future in favor of {@see gratum.source.OkHttpSource}
 *
 * A source that retrieves data from a URL.  For example,
 *
 * <pre>
 *  http("http://api.open-notify.org/astros.json").get().inject { Map json ->
 *     json.people
 *  }
 *  .filter([craft: "ISS"])
 *  .printRow()
 *  .go
 * </pre>
 *
 * To configure the http connection pass a closure that will pass
 * to {@link groovyx.net.http.HttpBuilder#configure(Closure)}.
 */
@Deprecated
class HttpSource extends AbstractSource {

    enum HttpVerb {
        GET,
        PUT,
        POST,
        HEAD,
        DELETE
    }

    String url
    HttpBuilder httpBuilder
    HttpVerb verb
    Closure requestConfiguration

    public HttpSource(String url) {
        super(url)
        this.url = url
    }

    public HttpSource(String url, HttpBuilder builder) {
        this( url )
        this.httpBuilder = builder
    }

    public HttpSource( HttpBuilder builder ) {
        super("http")
        this.httpBuilder = builder
    }

    public static HttpSource http( @DelegatesTo(HttpObjectConfig.class) Closure configuration ) {
        return new HttpSource( configure(configuration) )
    }

    public static HttpSource https( @DelegatesTo(HttpObjectConfig.class) Closure configuration ) {
        return new HttpSource( configure(configuration) )
    }

    public static HttpSource http(String url, HttpBuilder builder = null) {
        return new HttpSource(url, builder)
    }

    public static HttpSource https(String url, HttpBuilder builder = null) {
        return new HttpSource(url, builder)
    }

    public Pipeline get(@DelegatesTo(HttpConfig) Closure configuration = null) {
        verb = HttpVerb.GET
        requestConfiguration = configuration
        return into()
    }

    public Pipeline post(@DelegatesTo(HttpConfig) Closure configuration = null) {
        verb = HttpVerb.POST
        requestConfiguration = configuration
        return into()
    }

    public Pipeline delete(@DelegatesTo(HttpConfig) Closure configuration = null) {
        verb = HttpVerb.DELETE
        requestConfiguration = configuration
        return into()
    }

    public Pipeline put(@DelegatesTo(HttpConfig) Closure configuration = null) {
        verb = HttpVerb.PUT
        requestConfiguration = configuration
        return into()
    }

    public Pipeline head(@DelegatesTo(HttpConfig) Closure configuration = null) {
        verb = HttpVerb.HEAD
        requestConfiguration = configuration
        return into()
    }

    @Override
    void doStart(Pipeline pipeline) {
        if (!httpBuilder) {
            httpBuilder = configure() {
                request.uri = url
            }
        }
        switch (verb) {
            case HttpVerb.GET:
                pipeline.process( (Map) httpBuilder.get(createHttpConfigClosure()), 1 )
                break;
            case HttpVerb.POST:
                pipeline.process( (Map) httpBuilder.post(createHttpConfigClosure()), 1 )
                break;
            case HttpVerb.HEAD:
                pipeline.process( (Map) httpBuilder.head(createHttpConfigClosure()), 1 )
                break;
            case HttpVerb.PUT:
                pipeline.process(  (Map) httpBuilder.put(createHttpConfigClosure()), 1 )
                break;
            case HttpVerb.DELETE:
                pipeline.process( (Map) httpBuilder.delete(createHttpConfigClosure()), 1 )
                break;
            default:
                throw new IllegalAccessException("Unknown http verb.  Use one of get(), post(), put(), delete(), or head() methods.")
        }
    }

    Closure createHttpConfigClosure() {
        return {
            if( url ) request.uri = url
            if (requestConfiguration) {
                requestConfiguration.delegate = delegate
                requestConfiguration()
            }
        }
    }
}
