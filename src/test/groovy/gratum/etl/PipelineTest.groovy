package gratum.etl

import org.junit.Test

import static junit.framework.TestCase.*
import static gratum.source.HttpSource.*
import static gratum.source.CollectionSource.*

import static gratum.operators.Operators.*

/**
 * Created by charliehubbard on 7/13/18.
 */

class PipelineTest {

    List<Map> people = [
            [id: 1, name: 'Bill Rhodes', age: 53, gender: 'male'],
            [id: 2, name: 'Cheryl Lipscome', age: 43, gender: 'female'],
            [id: 3, name: 'Diana Rogers', age: 34, gender: 'female'],
            [id: 4, name: 'Jack Lowland', age: 25, gender: 'male'],
            [id: 5, name: 'Ginger Rogers', age: 83, gender: 'female']
    ]

    Collection<Map> hobbies = [
            [id:1, hobby: 'Stamp Collecting'],
            [id:1, hobby: 'Bird Watching'],
            [id:2, hobby: 'Biking'],
            [id:2, hobby: 'Tennis'],
            [id:3, hobby: 'Archeology'],
            [id:3, hobby: 'Treasure Hunting'],
            [id:4, hobby: 'Crossfit'],
            [id:4, hobby: 'Obstacle Races']
    ]


    @Test
    public void testSimpleFilter() {
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
            .filter([Sex:"male"])
            .onRejection { Pipeline pipeline ->
                pipeline.addStep("Verify sex was filtered out") { Rejection<Map<String,Object>> rej ->
                    assertFalse( rej.source.Sex == "male")
                    return rej
                }
                return
            }
            .go()

        assertNotNull( statistic )
        assertEquals( "Assert that we successfully loaded ", statistic.name, "src/test/resources/titanic.csv" )
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
            .onRejection { Pipeline<Rejection> pipeline ->
                pipeline.addStep("Verify sex was filtered out") { Rejection<Map<String,Object>> rej ->
                    assertFalse( "Assert Age = ${rej.source.Age} >= 30.0", rej.source.Age && (rej.source.Age as double) < 30.0 )
                    return rej
                }
                return
            }
            .go()

        assertNotNull( statistic )
        assertEquals( "Assert that we successfully loaded ", statistic.name, "src/test/resources/titanic.csv" )
        assertEquals( "Assert that we successfully loaded all male passengers", 185, statistic.loaded )
        assertEquals( "Assert that we rejected non-male passengers", 233, statistic.rejections )
        assertEquals( "Assert the rejection category", 233, statistic.getRejections(RejectionCategory.IGNORE_ROW) )
    }

    @Test
    public void testSimpleGroupBy() {
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
            .groupBy("Sex")
            .addStep("Assert groupBy(Sex)") { Map row ->
                assertEquals(266, row.male?.size())
                assertEquals(152, row.female?.size())
                return row
            }
            .go()

        assertEquals("Assert rows loaded == 1", 1, statistic.loaded )
        assertEquals("Assert rows rejected == 0", 0, statistic.rejections )
    }

    @Test
    public void testIntersect() {

    }

    @Test
    public void testConcat() {
        LoadStatistic stats = from([
                [name: 'Chuck', atBats: '200', hits: '100', battingAverage: '0.5'],
                [name: 'Sam', atBats: '300', hits: '125', battingAverage: '0.4166']
        ]).concat( from([
                [name: 'Rob', atBats: '100', hits: '75', battingAverage: '0.75'],
                [name: 'Sean', atBats: '20', hits: 'none', battingAverage: 'none']
        ])).go()

        assertEquals( 4, stats.loaded )
        assertEquals( 0, stats.rejections )
    }

    @Test
    public void testTrim() {
        LoadStatistic stats = from([
                [firstName: 'Roger   ', lastName: '  Smith'],
                [firstName: '    Jill', lastName: 'Belcher'],
                [firstName: '\tRick\t', lastName: 'Spade   ']
        ])
        .trim()
        .addStep("Assert all values were trimmed") { Map row ->
            assertFalse( row.firstName.contains(' '))
            assertFalse( row.firstName.contains('\t'))
            assertFalse( row.lastName.contains(' '))
            assertFalse( row.lastName.contains('\t'))
            return row
        }
        .go()
    }

    @Test
    public void testSetField() {
        LoadStatistic stats = from( people )
                .setField("completed", true)
                .addStep("Assert completed is defined") { Map row ->
                    assertTrue( row.completed )
                    return row
                }
                .go()
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
    public void testInnerJoin() {
        int rejections = 0
        LoadStatistic stats = from(people).join( from(hobbies), ['id'] )
            .addStep("Assert hobbies") { Map row ->
                assertNotNull( row.hobby )
                return row
            }
            .onRejection { Pipeline pipeline ->
                pipeline.addStep("Verify rejection") { Rejection<Map<String,Object>> rejection ->
                    rejections++
                    return rejection
                }
                return
            }
            .go()

        assertEquals( 8, stats.loaded )
        assertEquals( 1, stats.rejections )
        assertEquals( 1, rejections )
    }

    @Test
    public void testLeftJoin() {
        LoadStatistic stats = from(people).join( from(hobbies), ['id'], true )
            .addStep("Assert optional hobbies") { Map row ->
                if( row.id < 5 ) {
                    assertNotNull( row.hobby )
                } else {
                    assertNull( row.hobby )
                }
                return row
            }
            .go()

        assertEquals( 9, stats.loaded )
        assertEquals( 0, stats.rejections )
    }

    @Test
    public void testSort() {
        String lastHobby;
        from(hobbies).sort("hobby").addStep("Assert order is increasing") { Map row ->
            if( lastHobby ) assertTrue( "Assert ${lastHobby} < ${row.hobby}", lastHobby.compareTo( row.hobby ) <= 0 )
            lastHobby = row.hobby;
            return row
        }.go()

        assertNotNull("Assert that lastHobby is not null meaning we executing some portion of the assertions above.", lastHobby)
    }

    @Test
    public void testUnique() {
        LoadStatistic stats = from(hobbies).unique("id")
        .go()

        assertEquals( 4, stats.loaded )
        assertEquals( 4, stats.rejections )
        assertEquals( 4, stats.getRejections(RejectionCategory.IGNORE_ROW) )
    }

    @Test
    public void testRejections() {
        List<Rejection> rejections = []

        LoadStatistic stats = from(hobbies)
                .addStep("Rejection") { Map row -> row.id > 1 ? row : reject("${row.id} is too small", RejectionCategory.REJECTION) }
                .onRejection { Pipeline pipeline ->
                    pipeline.addStep("Save rejections") { Rejection<Map<String,Object>> row ->
                        rejections << row
                        return row
                    }
                    return null
                }
                .go()

        assertEquals( 6, stats.loaded )
        assertEquals( 2, stats.rejections )
        assertEquals( 2, stats.getRejections(RejectionCategory.REJECTION) )
        assertEquals( 2, rejections.size() )
    }

    @Test
    public void testHttp() {
        String message = null;
        int actualCount = 0;
        int expectedCount = 0;
        LoadStatistic stats = http("http://api.open-notify.org/astros.json").inject { Map json ->
            expectedCount = json.number
            message = json.message
            json.people
        }.addStep("assert astros in space") { Map row ->
            actualCount++
            // assert that we received the data we expected, but we can't really test anything because this will change over time
            assertNotNull( row.name )
            assertNotNull( row.craft )
            return row
        }.go()

        assertNotNull( "Message should be non-null if we called the service", message )
        assertEquals( "success", message )
        assertEquals( expectedCount, stats.loaded )
        // provided someone is in space!
        if( expectedCount > 0 ) {
            assertEquals( expectedCount, actualCount )
        }
    }

    @Test
    public void testAsDate() {
        LoadStatistic stats = from([
                [name: 'Chuck', dateOfBirth: '1992-08-11'],
                [name: 'Sam', dateOfBirth: '1980-04-12'],
                [name: 'Rob', dateOfBirth: 'unknown'],
                [name: 'Sean' ]
        ]).asDate('dateOfBirth')
        .go()

        assertEquals( 3, stats.loaded )
        assertEquals( 1, stats.rejections )
        assertEquals( 1, stats.getRejections(RejectionCategory.INVALID_FORMAT) )
    }

    @Test
    public void testAsInt() {
        LoadStatistic stats = from([
                [name: 'Chuck', atBats: '200', hits: '100'],
                [name: 'Sam', atBats: '300', hits: '125'],
                [name: 'Rob', atBats: '100', hits: '75'],
                [name: 'Sean', atBats: '20', hits: 'none']
        ]).asInt('hits')
        .go()

        assertEquals( 3, stats.loaded )
        assertEquals( 1, stats.rejections )
        assertEquals( 1, stats.getRejections(RejectionCategory.INVALID_FORMAT))
    }

    @Test
    public void testAsDouble() {
        LoadStatistic stats = from([
                [name: 'Chuck', atBats: '200', hits: '100', battingAverage: '0.5'],
                [name: 'Sam', atBats: '300', hits: '125', battingAverage: '0.4166'],
                [name: 'Rob', atBats: '100', hits: '75', battingAverage: '0.75'],
                [name: 'Sean', atBats: '20', hits: 'none', battingAverage: 'none']
        ]).asDouble('battingAverage')
                .go()

        assertEquals( 3, stats.loaded )
        assertEquals( 1, stats.rejections )
        assertEquals( 1, stats.getRejections(RejectionCategory.INVALID_FORMAT))
    }

    @Test
    public void testAsBoolean() {
        from([
                [name: 'Chuck', member: 'Y'],
                [name: 'Sue', member: 'y'],
                [name: 'Bill', member: 'YES'],
                [name: 'Don', member: 'yes'],
                [name: 'Kyle', member: 't'],
                [name: 'Greg', member: 'T'],
                [name: 'Jill', member: 'true'],
                [name: 'Kate', member: 'TRUE'],
                [name: 'Lily', member: '1']
        ])
        .asBoolean('member')
        .addStep("Assert all are boolean true") { Map row ->
            assertTrue( row.member instanceof Boolean )
            assertTrue( row.member )
            return row
        }.go()

        from([
                [name: 'Pat', member: 'N'],
                [name: 'Chuck', member: 'n'],
                [name: 'Sue', member: 'NO'],
                [name: 'Bill', member: 'no'],
                [name: 'Kyle', member: 'f'],
                [name: 'Greg', member: 'F'],
                [name: 'Jill', member: 'false'],
                [name: 'Kate', member: 'FALSE'],
                [name: 'Lily', member: '0']
        ])
        .asBoolean('member')
        .addStep("Assert all are boolean true") { Map row ->
            assertTrue( row.member instanceof Boolean )
            assertFalse( row.member )
            return row
        }.go()

    }

    @Test
    public void testSort2() {
        Pipeline<Map<String,Object>> p = from( people ) >> filterFields([gender: 'male']) >> sort('age')
        LoadStatistic stats = p.go()

        assertEquals( 2, stats.loaded )
        assertEquals( 3, stats.rejections )
        assertEquals( people.size(), stats.loaded + stats.rejections )
    }

    @Test
    public void testNewStyle() {
        Pipeline p = from( people ) >> filterFields( [gender: 'female'] ) >> { Map row ->
            assertTrue( row.gender == "female" )
            return row
        }
        LoadStatistic stats = p.go()

        assertEquals( 3, stats.loaded )
        assertEquals( 2, stats.rejections )
        assertEquals( people.size(), stats.loaded + stats.rejections )
    }

}
