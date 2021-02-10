package gratum.source

import gratum.etl.LoadStatistic
import org.junit.Test

class XlsxSourceTest {

    @Test
    void testXlsxLoading() {
        int id = 1
        LoadStatistic stat = XlsxSource.xlsx( "Players", Class.getResourceAsStream("/players.xlsx") ).into()
            .addStep("Verify") { Map row ->
                assert row.size() == 5 // should have 5 columns
                row.each { String col, Object value ->
                    assert value != null
                }
                row.id == id
                id++
                return row
            }
            .go()

        assert stat.loaded == 6
        assert stat.rejections == 0
    }

    @Test
    void testXlsxGroupBy() {
        LoadStatistic stat = XlsxSource.xlsx("Players", Class.getResourceAsStream("/players.xlsx")).into()
            .groupBy("color")
                    .addStep("Verify groups") { Map<String,List<Map<String,Object>>> row ->
                assert row.size() == 5
                assert row.green.size() == 2
                assert row.blue.size() == 1
                assert row.purple.size() == 1
                return row
            }
            .go()

        assert stat.loaded == 1
        assert stat.rejections == 0
    }
}
