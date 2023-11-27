package gratum.source

import gratum.etl.LoadStatistic
import org.junit.Test

import static gratum.source.HttpSource.*

import static junit.framework.TestCase.assertNotNull

class HttpSourceTest {

    @Test(timeout = 5000L)
    public void testHttp() {
        String message = null
        int actualCount = 0
        int expectedCount = 0
        LoadStatistic stats = http("http://api.open-notify.org/astros.json").get()
                .inject { json ->
                    expectedCount = json.number
                    message = json.message
                    json.people
                }.addStep("assert astros in space") { row ->
            actualCount++
            // assert that we received the data we expected, but we can't really test anything because this will change over time
            assertNotNull( row.name )
            assertNotNull( row.craft )
            return row
        }.go()

        assertNotNull( "Message should be non-null if we called the service", message )
        assert message == "success"
        assert stats.loaded == expectedCount
        // provided someone is in space!
        if( expectedCount > 0 ) {
            assert actualCount == expectedCount
        }
    }

}
