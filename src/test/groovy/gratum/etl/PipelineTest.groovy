package gratum.etl

import static junit.framework.TestCase.*
import org.junit.Test

/**
 * Created by charliehubbard on 7/13/18.
 */

class PipelineTest {

    @Test
    public void testSimpleFilter() {
        LoadStatistic statistic = Pipeline.csv("src/test/resources/titanic.csv")
            .filter([Sex:"male"])
            .onRejection { Pipeline rej ->
                rej.addStep("Verify sex was filtered out") { Map row ->
                    assertFalse( row.Sex == "male")
                    return row
                }
                return
            }
            .go()

        assertNotNull( statistic )
        assertEquals( "Assert that we successfully loaded ", statistic.filename, "src/test/resources/titanic.csv" )
        assertEquals( "Assert that we successfully loaded all male passengers", 258, statistic.loaded )
        assertEquals( "Assert that we rejected non-male passengers", 160, statistic.rejections )
        assertEquals( "Assert the rejection category", 160, statistic.getRejections(RejectionCategory.IGNORE_ROW) )
    }

    @Test
    public void testSimpleFilterClosure() {
        LoadStatistic statistic = Pipeline.csv("src/test/resources/titanic.csv")
                .filter() { Map row ->
                    return row.Age && (row.Age as double) < 30.0
                }
                .onRejection { Pipeline rej ->
            rej.addStep("Verify sex was filtered out") { Map row ->
                assertFalse( "Assert Age = ${row.Age} >= 30.0", row.Age && (row.Age as double) < 30.0 )
                return row
            }
            return
        }
        .go()

        assertNotNull( statistic )
        assertEquals( "Assert that we successfully loaded ", statistic.filename, "src/test/resources/titanic.csv" )
        assertEquals( "Assert that we successfully loaded all male passengers", 176, statistic.loaded )
        assertEquals( "Assert that we rejected non-male passengers", 242, statistic.rejections )
        assertEquals( "Assert the rejection category", 242, statistic.getRejections(RejectionCategory.IGNORE_ROW) )
    }

    @Test
    public void testIntersect() {

    }

    @Test
    public void testConcat() {

    }

    @Test
    public void renameFields() {

    }

    @Test
    public void testFillDownBy() {

    }

    @Test
    public void testBranch() {

    }

    @Test
    public void testJoin() {

    }

    @Test
    public void testSimpleGroupBy() {

    }

}
