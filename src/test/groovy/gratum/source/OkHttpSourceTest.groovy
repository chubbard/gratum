package gratum.source

import gratum.etl.LoadStatistic
import org.junit.Test

import static gratum.source.OkHttpSource.*
import static junit.framework.TestCase.assertNotNull

class OkHttpSourceTest {

    @Test(timeout = 10_000L)
    void testOkHttpSource() {

        String message = null
        int actualCount = 0
        int expectedCount = 0
        LoadStatistic stats = http("http://api.open-notify.org/astros.json") {
                get()
            }
            .into()
            .addStep("Assert Response,Body,Status") { row ->
                assert row.response != null
                assert row.status == 200
                assert row.body != null
                return row
            }
            .inject { row ->
                expectedCount = row.json.number
                message = row.json.message
                row.json.people
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

    @Test(timeout = 5000L)
    void testOkHttps() {
        LoadStatistic stats = https("https://postman-echo.com/get?color=green&mode=lit")
            .into()
            .addStep("Assert Response,Body,Status") { row ->
                assert row.response != null
                assert row.status == 200
                assert row.body != null
                return row
            }
            .addStep("Assert JSON property") { row ->
                assert row.json != null
                assert row.json.args["color"] == "green"
                assert row.json.args["mode"] == "lit"
                return row
            }
            .go()

        assert stats.loaded == 1
        assert stats.rejections == 0
    }

    @Test(timeout = 5000L)
    void testOkHttpXml() {
        LoadStatistic stats = https("https://www.purgomalum.com/service/xml?text=Is+this+a+good+idea+to+use+query+params+or+a+shitty+one%3F") {
            header("Accept", "text/xml")
            header("Accept", "application/xml")
        }
        .into()
        .addStep("Asert XML") { row ->
            assert row.xml != null
            return row
        }
        .go()
        assert stats.loaded == 1
        assert stats.rejections == 0
    }

    @Test
    void testQueryParams() {
        LoadStatistic stats = https("https://postman-echo.com/get") {
                    query( page: 6, limit: 100)
                }
                .into()
                .addStep("Assert Response,Body,Status") { row ->
                    assert row.response != null
                    assert row.status == 200
                    assert row.body != null
                    return row
                }
                .addStep("Assert JSON property") { row ->
                    assert row.json != null
                    assert row.json.args["page"] == "6"
                    assert row.json.args["limit"] == "100"
                    return row
                }
                .go()

        assert stats.loaded == 1
        assert stats.rejections == 0
    }
}
