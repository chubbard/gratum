package gratum.source

import gratum.etl.LoadStatistic
import org.junit.Test

class XlsxSourceTest {

    @Test
    void testXlsxLoading() {
        int id = 1
        LoadStatistic stat = XlsxSource.xlsx( "Players", Class.getResourceAsStream("/players.xlsx") ).into()
            .asInt("ID")
            .addStep("Verify") { Map row ->
                assert row.size() == 6 // should have 6 columns
                if( row.ID != 7 ) {
                    row.each { String col, Object value ->
                        assert value != null
                    }
                }
                assert row.ID == id
                id++
                return row
            }
            .go()

        assert stat.loaded == 7
        assert stat.rejections == 0
    }

    @Test
    void testXlsxGroupBy() {
        LoadStatistic stat = XlsxSource.xlsx("Players", Class.getResourceAsStream("/players.xlsx")).into()
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

    @Test
    void testXlsxDates() {
        LoadStatistic stat = XlsxSource.xlsx("Players", Class.getResourceAsStream("/players.xlsx")).into()
            .asDate("birth_date", "MM/dd/yyyy")
            .addStep("Verify Dates") { Map<String,Object> row ->
                assert row.birth_date instanceof Date
                return row
            }
            .go()

        assert stat.loaded == 7
        assert stat.rejections == 0
    }

}
