package gratum.source

import static groovyx.net.http.HttpBuilder.configure

import gratum.etl.Pipeline

/**
 * A source that retrieves data from a URL.  For example,
 *
 * <pre>
 *  http("http://api.open-notify.org/astros.json").inject { Map json ->
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
class HttpSource implements Source {

    String url
    Closure configuration

    public HttpSource(String url, Closure configuration) {
        this.url = url
        this.configuration = configuration
    }

    public static Pipeline http(String url, Closure configuration = null) {
        Pipeline pipeline = new Pipeline( url )
        pipeline.src = new HttpSource(url, configuration)
        return pipeline
    }

    public static Pipeline https(String url, Closure configuration = null) {
        Pipeline pipeline = new Pipeline(url)
        pipeline.src = new HttpSource(url, configuration)
        return pipeline
    }

    @Override
    void start(Pipeline pipeline) {
        def response = configure {
            request.uri = url
            if( configuration ) {
                configuration.delegate = delegate
                configuration()
            }
        }.get()

        pipeline.process( (Map)response, 1 )
    }
}
