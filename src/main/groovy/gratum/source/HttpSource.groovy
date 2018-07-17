package gratum.source

import static groovyx.net.http.HttpBuilder.configure

import gratum.etl.Pipeline

/**
 * Created by charliehubbard on 7/16/18.
 */
class HttpSource implements Source {

    String url

    public HttpSource(String url) {
        this.url = url
    }

    public static Pipeline http(String url) {
        Pipeline pipeline = new Pipeline( url )
        pipeline.src = new HttpSource(url)
        return pipeline
    }

    public static Pipeline https(String url) {
        Pipeline pipeline = new Pipeline(url)
        pipeline.src = new HttpSource(url)
        return pipeline
    }

    @Override
    void start(Closure closure) {
        def result = HttpBuilder.configure {
            request.raw = url
        }.get()
    }
}
