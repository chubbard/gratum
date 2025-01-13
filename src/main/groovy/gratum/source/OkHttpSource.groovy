package gratum.source

import gratum.etl.Pipeline
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import okhttp3.Credentials
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request

/*
 * This source fetches data from a URL using the OkHttp library and posts
 * the results to a Pipeline.
 *
 * <pre>
 *  http("http://api.open-notify.org/astros.json").into()
 *  .addStep("Swap Response -> Json") { row ->
 *     row.json
 *  }
 *  .filter([craft: "ISS"])
 *  .printRow()
 *  .go
 * </pre>
 *
 * For URLs that return "application/json" or either "text/xml" or "application/xml" the source will
 * process the body using JsonSlurper or XmlSlurper and return properties for
 * json and xml, respectively.
 */
@CompileStatic
class OkHttpSource extends AbstractSource {

    HttpUrl url
    OkHttpClient client = new OkHttpClient()
    Closure configure

    /**
     * A wrapper class around OKHttp's Request.Builder for building the request being sent
     * in an Http call.  This class adds helper methods not present in OkHttp's Request.Builder.
     */
    static class OkHttpBuilder {

        public HttpUrl url

        public OkHttpClient client

        @Delegate
        Request.Builder builder

        OkHttpBuilder(OkHttpClient client, Request.Builder builder, HttpUrl url) {
            this.client = client
            this.builder = builder
            this.url = url
        }

        Request build() {
            builder.url( url )
            builder.build()
        }

        /**
         * Helper method to add Basic authentication header to the request.
         * @param username the given username to authenticate
         * @param password the given password to authenticate
         * @return this instance
         */
        OkHttpBuilder authBasic(String username, String password) {
            builder.header("Authorization", Credentials.basic(username, password))
            return this
        }

        OkHttpBuilder authToken(String jwtToken) {
            builder.header("Authorization", "Bearer ${jwtToken}")
            return this
        }

        OkHttpBuilder query(Map params) {
            HttpUrl.Builder urlBuilder = url.newBuilder()
            params.each { x,y -> urlBuilder.addQueryParameter(x as String,y as String) }
            url = urlBuilder.build()
            return this
        }
    }

    OkHttpSource(String url, Closure configure) {
        super(url)
        this.url = HttpUrl.parse(url)
        this.configure = configure
    }

    static OkHttpSource http( String url, @DelegatesTo(OkHttpBuilder) Closure configure = null ) {
        return new OkHttpSource( url, configure )
    }

    static OkHttpSource https( String url, @DelegatesTo(OkHttpBuilder) Closure configure = null ) {
        return new OkHttpSource( url, configure )
    }

    @Override
    void doStart(Pipeline pipeline) {
        OkHttpBuilder wrapper = new OkHttpBuilder(client, new Request.Builder(), this.url)
        if( configure ) {
            configure.delegate = wrapper
            configure()
        }
        wrapper.client.newCall(wrapper.build()).execute().withCloseable { response ->
            Map<String,Object> result = [response: response, body: response.body(), status: response.code()]
            String contentType = response.header("Content-Type")?.split(";")?.first()
            switch(contentType) {
                case "application/json":
                    JsonSlurper json = new JsonSlurper()
                    result.json = json.parse( response.body().charStream() )
                    break
                case "application/xml":
                case "text/xml":
                    XmlSlurper xml = new XmlSlurper()
                    result.xml = xml.parse( response.body().charStream() )
                    break
            }
            pipeline.process(result,1)
        }
    }
}
