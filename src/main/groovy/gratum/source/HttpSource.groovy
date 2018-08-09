package gratum.source

import static groovyx.net.http.HttpBuilder.configure

import gratum.etl.Pipeline

/**
 * Created by charliehubbard on 7/16/18.
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
    void start(Closure closure) {
        def response = configure {
            request.uri = url
            if( configuration ) {
                configuration.delegate = delegate
                configuration()
            }
        }.get()

        closure( response )
    }
}
