package gratum.source

import gratum.etl.LoadStatistic
import org.junit.Test

class XlsSourceTest {

    @Test
    void testReadingXls() {
        LoadStatistic stat = XlsSource.xls( "Players", Class.getResourceAsStream("/players.xls") ).into()
            .addStep("validate players row") { Map<String,Object> row ->
                assert row.size() == 6
                row.each { String col, Object value ->
                    assert (row["ID"] == '7' && col == 'score') || value != null
                }
                return row
            }
            .go()

        assert stat.loaded == 7
        assert stat.rejections == 0
    }

    @Test
    void testXlsGroupBy() {
        LoadStatistic stat = XlsSource.xls("Players", Class.getResourceAsStream("/players.xls")).into()
                .groupBy("color")
                .addStep("Verify groups") { Map<String,List<Map<String,Object>>> row ->
                    assert row.size() == 6
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
