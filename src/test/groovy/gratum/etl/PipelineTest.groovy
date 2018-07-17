package gratum.etl

import org.junit.Test

import static junit.framework.TestCase.*
import static gratum.source.CsvSource.*

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
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
            .branch { Pipeline p ->
                p.groupBy(["Sex"])
                        .addStep("Assert groupBy(Sex)") { Map row ->
                    if (row.Sex == "male") {
                        assertEquals(266, row.count)
                    } else if (row.Sex == "female") {
                        assertEquals(152, row.count)
                    }
                    return row
                }
                null
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
        int count = 0
        csv("src/test/resources/fill_down.csv")
            .addStep("Assert fields are missing data.") { Map row ->
                if( !row.first_name ) {
                    count++
                    assertTrue("Assert first_name is empty or null", !row.first_anem)
                    assertTrue("Assert last_name is empty or null", !row.last_name)
                    assertTrue("Assert date_of_birth is empty or null", !row.date_of_birth)
                }
                return row
            }
            .fillDownBy { Map row, Map previousRow ->
                return row.id == previousRow.id
            }
            .addStep("Assert values were filled down") { Map row ->
                row.each { String key, Object value ->
                    assertNotNull( "Assert ${key} is filled in with a value", value )
                    assertTrue("Assert that ${key} is non-empty", !(value as String).isEmpty() )
                }
                return row
            }
            .go()

        assertTrue("Assert that we encountered rows that weren't filled in", count > 0 )
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
