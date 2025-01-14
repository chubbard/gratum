package gratum.source

import gratum.etl.Pipeline
import gratum.etl.RejectionCategory
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
 * {@code
 *  http("http://api.open-notify.org/astros.json").into()
 *  .addStep("Swap Response -> Json") { row ->
 *     row.json
 *  }
 *  .filter([craft: "ISS"])
 *  .printRow()
 *  .go
 * }
 * </pre>
 * <p>
 * The row played down the Pipeline when using this source will consist of the following columns:
 *</p>
 * <p>
 * <ul>
 * <li>url - the URL visited</li>
 * <li>response - a OkHttp response object.</li>
 * <li>body - The OkHttp Body object of the response (ie response.body())</li>
 * <li>status - The HTTP status code returned (ie response.code())</li>
 * <li>json - A parsed json object returned from JsonSlurper if the Content-Type returned is "application/json"</li>
 * <li>xml - A parse xml object returned from XmlSlurper if the Content-Type returned is "text/xml" or "application/xml"</li>
 * </ul>
 * </p>
 */
@CompileStatic
class OkHttpSource extends AbstractSource {

    public static final int MAX_RETRIES = 5

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

        /**
         * Helper method to add Bearer authentication header to the request.
         *
         * @param jwtToken the token to add to the header
         * @return this instance
         */
        OkHttpBuilder authToken(String jwtToken) {
            builder.header("Authorization", "Bearer ${jwtToken}")
            return this
        }

        /**
         * Closure to configure the client for this OkHttpSource.  This allows you to override the default client
         * configuration for this use of OkHttpSource.
         */
        OkHttpBuilder client(@DelegatesTo(OkHttpClient.Builder) Closure configure) {
            client = client.newBuilder().with {
                configure.delegate = it
                configure()
                return it
            }.build()
            return this
        }

        /**
         * Helper method to add query parameters to the existing url.
         * @param params A Map containing the parameter names and values
         * @return this instance
         */
        OkHttpBuilder query(Map<String,Object> params) {
            HttpUrl.Builder urlBuilder = url.newBuilder()
            params.each { x,y -> urlBuilder.addQueryParameter(x,y as String) }
            url = urlBuilder.build()
            return this
        }
    }

    /**
     * The constructor of OkHttpSource used internally.  Use one of the static helper methods when constructing an
     * instance of this type.
     * @param url
     * @param client
     * @param configure
     */
    OkHttpSource(String url, OkHttpClient client, Closure configure) {
        super(url)
        this.url = HttpUrl.parse(url)
        this.configure = configure
        this.client = client
    }

    /**
     * This creates an OkHttpSource with its own OkHttpClient which won't be shared.  This is only useful
     * when using only 1 OkHttpSource.  If you have multiple OkHttpSources within your pipeline or you execute
     * multiple calls consider shared the OkHttpClient instance between them using:
     *
     * {@link OkHttpSource#http(java.lang.String, okhttp3.OkHttpClient, groovy.lang.Closure)}
     *
     * @param url the url to load
     * @param configure a closure to configure the request or other aspects of this call.
     * @return The OkHttpSource
     */
    static OkHttpSource http( String url, @DelegatesTo(OkHttpBuilder) Closure configure = null ) {
        return http( url, new OkHttpClient(), configure )
    }

    /**
     * Best practice is to share OkHttpClients when performing multiple http calls or multiple OkHttpSources.  This
     * enables sharing the OkHttpClient across multiple OkHttpSource instances.
     * @param url the url to load
     * @param client the shared OkHttpClient instance
     * @param configure a closure to configure the request or other aspects of this call.
     * @return The OkHttpSource
     */
    static OkHttpSource http(String url, OkHttpClient client, @DelegatesTo(OkHttpBuilder) Closure configure = null ) {
        return new OkHttpSource(url, client, configure)
    }

    /**
     * This creates an OkHttpSource with its own OkHttpClient which won't be shared.  This is only useful
     * when using only 1 OkHttpSource.  If you have multiple OkHttpSources within your pipeline or you execute
     * multiple calls consider shared the OkHttpClient instance between them using:
     *
     * {@link OkHttpSource#https(java.lang.String, okhttp3.OkHttpClient, groovy.lang.Closure)}
     *
     * @param url the url to load
     * @param configure a closure to configure the request or other aspects of this call.
     * @return The OkHttpSource
     */
    static OkHttpSource https( String url, @DelegatesTo(OkHttpBuilder) Closure configure = null ) {
        return https( url, new OkHttpClient(), configure )
    }

    /**
     * Best practice is to share OkHttpClients when performing multiple http calls or multiple OkHttpSources.  This
     * enables sharing the OkHttpClient across multiple OkHttpSource instances.
     * @param url the url to load
     * @param client the shared OkHttpClient instance
     * @param configure a closure to configure the request or other aspects of this call.
     * @return The OkHttpSource
     */
    static OkHttpSource https(String url, OkHttpClient client, @DelegatesTo(OkHttpBuilder) Closure configure = null) {
        return new OkHttpSource( url, client, configure )
    }

    @Override
    void doStart(Pipeline pipeline) {
        OkHttpBuilder wrapper = new OkHttpBuilder(client, new Request.Builder(), this.url)
        if( configure ) {
            configure.delegate = wrapper
            configure()
        }
        boolean done = false
        int attempts = MAX_RETRIES
        while( !done && attempts > 0) {
            wrapper.client.newCall(wrapper.build()).execute().withCloseable { response ->
                attempts--
                if (response.code() == 429) {
                    Integer retryAfter = response.header("Retry-After") as Integer
                    Thread.sleep(retryAfter * 1000L)
                    done = false
                } else {
                    Map<String, Object> result = [url: wrapper.url, response: response, body: response.body(), status: response.code()]
                    String contentType = response.header("Content-Type")?.split(";")?.first()
                    switch (contentType) {
                        case "application/json":
                            JsonSlurper json = new JsonSlurper()
                            result.json = json.parse(response.body().charStream())
                            break
                        case "application/xml":
                        case "text/xml":
                            XmlSlurper xml = new XmlSlurper()
                            result.xml = xml.parse(response.body().charStream())
                            break
                    }
                    pipeline.process(result, 1)
                    done = true
                }

                if( !done && attempts == 0 ) {
                    pipeline.reject(Pipeline.reject([url: wrapper.url, response: response, body: null, status: response.code()],
                            "Maximum attempts reach (${MAX_RETRIES})",
                            RejectionCategory.REJECTION),
                            1 )
                }
            }
        }
    }
}
