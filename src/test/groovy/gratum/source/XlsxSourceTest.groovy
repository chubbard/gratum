package gratum.source

import gratum.etl.LoadStatistic
import gratum.etl.Pipeline
import org.junit.Test

class XlsxSourceTest {


    @Test
    void testXlsxLoading() {
        int id = 1
        LoadStatistic stat = XlsxSource.xlsx( "Players", Class.getResourceAsStream("/players.xlsx") ).attach { Pipeline pipeline ->
            pipeline.addStep("Verify") { Map row ->
                assert row.size() == 5 // should have 5 columns
                row.each { String col, Object value ->
                    assert value != null
                }
                row.id == id
                id++
                return row
            }
            return pipeline
        }

        assert stat.loaded == 6
        assert stat.rejections == 0
    }

    @Test
    void testXlsxGroupBy() {
        LoadStatistic stat = XlsxSource.xlsx("Players", Class.getResourceAsStream("/players.xlsx")).attach { Pipeline pipeline ->
            return pipeline.groupBy("color")
                    .addStep("Verify groups") { Map<String,List<Map<String,Object>>> row ->
                assert row.size() == 5
                assert row.green.size() == 2
                assert row.blue.size() == 1
                assert row.purple.size() == 1
                return row
            }
        }

        assert stat.loaded == 1
        assert stat.rejections == 0
    }
}
