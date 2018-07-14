package gratum.etl

import org.junit.Test

import static junit.framework.TestCase.*
import static gratum.etl.Pipeline.*

/**
 * Created by charliehubbard on 7/13/18.
 */

class PipelineTest {

    @Test
    public void testSimpleFilter() {
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
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
        assertEquals( "Assert that we successfully loaded all male passengers", 266, statistic.loaded )
        assertEquals( "Assert that we rejected non-male passengers", 152, statistic.rejections )
        assertEquals( "Assert the rejection category", 152, statistic.getRejections(RejectionCategory.IGNORE_ROW) )
    }

    @Test
    public void testSimpleFilterClosure() {
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
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
        assertEquals( "Assert that we successfully loaded all male passengers", 185, statistic.loaded )
        assertEquals( "Assert that we rejected non-male passengers", 233, statistic.rejections )
        assertEquals( "Assert the rejection category", 233, statistic.getRejections(RejectionCategory.IGNORE_ROW) )
    }

    @Test
    public void testSimpleGroupBy() {
        // todo should we return the statistics for the original pipeline or should we return separate stats for the groupby?
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
                .groupBy(["Sex"])
                .addStep("Assert groupBy(Sex)") {Map row ->
            if( row.Sex == "male" ) {
                assertEquals( 266, row.count )
            } else if( row.Sex == "female" ) {
                assertEquals( 152, row.count )
            }
            return row
        }
        .go()

        assertEquals("Assert rows loaded == 418", 418, statistic.loaded )
        assertEquals("Assert rows rejected == 0", 0, statistic.rejections )
    }

    @Test
    public void testIntersect() {

    }

    @Test
    public void testConcat() {

    }

    @Test
    public void renameFields() {
        csv("src/test/resources/titanic.csv")
            .addStep("Test Sex Exists") { Map row ->
                assertTrue("Assert row.Sex exists", row.containsKey("Sex"))
                assertTrue("Assert row.Age exists", row.containsKey("Age"))
                return row
            }
            .renameFields([Sex: "gender", "Age": "age"])
            .addStep("Test Sex renamed to gender and Age to age") { Map row ->
                assertTrue( row.containsKey("gender") )
                assertTrue( row.containsKey("age") )
                return row
            }
            .go()
    }

    @Test
    public void testAddField() {
        csv("src/test/resources/titanic.csv")
            .addField("survived") { Map row ->
                return true
            }
            .addStep("Test Field added") { Map row ->
                assertTrue( row.containsKey("survived") )
                return row
            }
            .go()
    }

    @Test
    public void testFillDownBy() {

    }

    @Test
    public void testBranch() {
        csv("src/test/resources/titanic.csv")
            .branch { Pipeline pipeline ->
                pipeline.filter([Sex: "female"])
                    .addStep("Verify sex was filtered out") { Map row ->
                        assertTrue( row.Sex == "female" )
                        return row
                    }
                return null
            }
            .filter([Sex:"male"])
            .addStep("Verify sex was filtered to male") { Map row ->
                assertTrue( row.Sex == "male")
                return row
            }
            .go()
    }

    @Test
    public void testJoin() {

    }

}
