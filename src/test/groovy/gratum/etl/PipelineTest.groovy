package gratum.etl

import gratum.source.CollectionSource
import gratum.source.CsvSource
import org.junit.Test

import static junit.framework.TestCase.*
import static gratum.source.CsvSource.*
import static gratum.source.HttpSource.*
import static gratum.source.CollectionSource.*

/**
 * Created by charliehubbard on 7/13/18.
 */

class PipelineTest {

    @Test
    void testPrependStep() {
        int afterRows = 0, beforeRows = 0
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
            .filter([Sex: "male"])
            .addStep("Count the rows") { Map row ->
                afterRows++
                return row
            }
            .prependStep("Count the rows before") { Map row ->
                beforeRows++
                return row
            }
            .go()
        assert statistic != null
        assert statistic.loaded == 266
        assert afterRows == 266
        assert beforeRows > afterRows
    }

    @Test
    void testSimpleFilter() {
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
        assertEquals( "Assert that we successfully loaded ", statistic.name, "titanic.csv" )
        assertEquals( "Assert that we successfully loaded all male passengers", 266, statistic.loaded )
        assertEquals( "Assert that we rejected non-male passengers", 152, statistic.rejections )
        assertEquals( "Assert the rejection category", 152, statistic.getRejections(RejectionCategory.IGNORE_ROW) )
    }

    @Test
    void testSimpleFilterClosure() {
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
        assertEquals( "Assert that we successfully loaded ", statistic.name, "titanic.csv" )
        assertEquals( "Assert that we successfully loaded all male passengers", 185, statistic.loaded )
        assertEquals( "Assert that we rejected non-male passengers", 233, statistic.rejections )
        assertEquals( "Assert the rejection category", 233, statistic.getRejections(RejectionCategory.IGNORE_ROW) )
        assertEquals("Assert rejection step is 1", 1, statistic.getRejectionsFor(RejectionCategory.IGNORE_ROW).size() )
        assertEquals("Assert rejection step is filter()", "filter()", statistic.getRejectionsFor(RejectionCategory.IGNORE_ROW).keySet().first())
        assertEquals("Assert rejection step filter() == 233", 233, statistic.getRejections(RejectionCategory.IGNORE_ROW,"filter()"))
    }

    @Test
    void testFilterMapWithCollection() {
        List<String> filter = ["3", "2"]

        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
                .filter([Pclass: filter, Sex: "male"])
                .onRejection { Pipeline rej ->
                    rej.addStep("verify not sex != male && pClass != 3") { Map row ->
                        assert !filter.contains(row.Pclass) || row.Sex != "male"
                        return row
                    }
                    return
                }
                .go()

        assert statistic.loaded == 209
        assert statistic.rejections == 209
        assert statistic.getRejections(RejectionCategory.IGNORE_ROW) == 209
        assert statistic.getRejections( RejectionCategory.IGNORE_ROW, "filter Pclass -> [3, 2],Sex -> male" ) == 209
    }

    @Test
    void testFilterMap() {
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
            .filter([Pclass: "3", Sex: "male"])
            .onRejection { Pipeline rej ->
                rej.addStep("verify not sex != male && pClass != 3") { Map row ->
                    assert row.Pclass != "3" || row.Sex != "male"
                    return row
                }
                return
            }
            .go()

        assert statistic.loaded == 146
        assert statistic.rejections == 272
        assert statistic.getRejections(RejectionCategory.IGNORE_ROW) == 272
        assert statistic.getRejections( RejectionCategory.IGNORE_ROW, "filter Pclass -> 3,Sex -> male" ) == 272
    }

    @Test
    void testFilterMapWithClosure() {
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
            .filter([Pclass: { value -> (value as Integer) < 3 } ])
            .onRejection { Pipeline rej ->
                rej.addStep("Verify all rejections are == 3") { Map row ->
                    assert row.Pclass == "3"
                    return row
                }
                return
            }
            .go()

        assert statistic.loaded == 200
        assert statistic.rejections == 218
        assert statistic.getRejections( RejectionCategory.IGNORE_ROW ) == 218
    }

    @Test
    void testSimpleGroupBy() {
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
    void testMultipleGroupBy() {
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
            .asInt("Age")
            .filter() { Map row ->
                row.Age ? row.Age < 20 : false
            }
            .groupBy("Sex", "Pclass")
            .addStep( "Assert groupBy(Sex,Pclass") { Map row ->
                // size reflects how many sub-values there are for each group.
                // not total items fitting into this group, only
                // the leaves hold that information.
                assertEquals( 3, row.male?.size() )
                assertEquals( 3, row.female?.size() )

                assertEquals( 3, row.male['1'].size() )
                assertEquals( 7, row.male['2'].size() )
                assertEquals( 16, row.male['3'].size() )

                assertEquals( 2, row.female['1'].size() )
                assertEquals( 7, row.female['2'].size() )
                assertEquals( 16, row.female['3'].size() )

                return row
            }
            .go()

        // verify that step timings for steps before the groupBy() are included in the returned statistics
        assertTrue( "Assert that the timings include the asInt step", statistic.stepTimings.containsKey("asInt(Age)") )
        assertTrue( "Assert that the timings include the filter step", statistic.stepTimings.containsKey("filter()") )
        assertTrue( "Assert that the timings include the groupBy(Sex,Pclass) step", statistic.stepTimings.containsKey("groupBy(Sex,Pclass)") )
    }

    @Test
    void testEmptyGroupBy() {
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
            .filter([Sex: 'K'])
            .groupBy("Sex")
            .addStep("Assert groupBy(Sex)") { Map row ->
                assertTrue( row.isEmpty() )
                return row
            }
            .go()
        assertEquals( "Assert rows loaded == 1", 1, statistic.loaded )
        assertEquals( "Assert all rows rejected", 418, statistic.rejections )

        assertTrue( "Assert that the timings include the filter(Sex->K)) step", statistic.stepTimings.containsKey("filter Sex -> K") )
        // I'm not entirely sure why this assert was added.  It seems logical to include
        // it as it's a step on the Pipeline but it was done for specific reasons, but
        // those reasons are lost to history.  I'm leaving it in case I remember why this was
        // important or not.
//        assertFalse( "Assert that timings does NOT include groupBy because all rows are filtered out.", statistic.stepTimings.containsKey("groupBy(Sex)") )
        assertTrue( "Assert that timings does include groupBy because all rows are filtered out.", statistic.stepTimings.containsKey("groupBy(Sex)") )
    }

    @Test
    void testIntersect() {

    }

    @Test
    void testConcat() {
        int called = 0
        LoadStatistic stats = from([
                [name: 'Chuck', atBats: '200', hits: '100', battingAverage: '0.5'],
                [name: 'Sam', atBats: '300', hits: '125', battingAverage: '0.4166']
        ]).concat( from([
                [name: 'Rob', atBats: '100', hits: '75', battingAverage: '0.75'],
                [name: 'Sean', atBats: '20', hits: 'none', battingAverage: 'none']
        ]))
        .addStep("Assert we get rows") { Map row ->
            called++
            return row
        }
        .go()

        assertEquals( 4, stats.loaded )
        assertEquals( 0, stats.rejections )
        assertEquals( stats.loaded, called )
    }

    @Test
    void testTrim() {
        LoadStatistic stats = from([
                [firstName: 'Roger   ', lastName: '  Smith'],
                [firstName: '    Jill', lastName: 'Belcher'],
                [firstName: '\tRick\t', lastName: 'Spade   ']
        ])
        .trim()
        .addStep("Assert all values were trimmed") { Map row ->
            assert !row.firstName.contains(' ')
            assert !row.firstName.contains('\t')
            assert !row.lastName.contains(' ')
            assert !row.lastName.contains('\t')
            return row
        }
        .go()

        assert stats.loaded == 3
    }

    @Test
    void testSetField() {
        LoadStatistic stats = from( GratumFixture.people )
                .setField("completed", true)
                .addStep("Assert completed is defined") { Map row ->
                    assert row.completed
                    return row
                }
                .go()

        assert stats.loaded == 5
    }

    @Test
    void renameFields() {
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
    void testAddField() {
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
    void testFillDownBy() {
        int count = 0
        csv("src/test/resources/fill_down.csv")
            .addStep("Assert fields are missing data.") { Map row ->
                if( !row.first_name ) {
                    count++
                    assert !row.first_anem
                    assert !row.last_name
                    assert !row.date_of_birth
                }
                return row
            }
            .fillDownBy { Map row, Map previousRow ->
                return row.id == previousRow.id
            }
            .addStep("Assert values were filled down") { Map<String,Object> row ->
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
    void testBranch() {
        boolean branchEntered = false
        csv("src/test/resources/titanic.csv")
            .branch { Pipeline pipeline ->
                return pipeline.filter([Sex: "female"])
                    .addStep("Verify sex was filtered out") { Map row ->
                        assertTrue( row.Sex == "female" )
                        return row
                    }
                    .defaultValues("branch": true)
                    .addStep("Branch Entered") { Map row ->
                        assert row.branch
                        branchEntered = true
                        return row
                    }
            }
            .addStep("Verify branch field is NOT on the outter Pipeline") { Map row ->
                assert row.branch == null
                return row
            }
            .filter([Sex:"male"])
            .addStep("Verify sex was filtered to male") { Map row ->
                assertTrue( row.Sex == "male")
                return row
            }
            .go()
        assert branchEntered
    }

    @Test
    void testBranchWithGroupBy() {
        csv("src/test/resources/titanic.csv")
            .branch { Pipeline p ->
                return p.groupBy("Sex", "Pclass").addStep { Map row ->
                    assertNotNull( row["male"] )
                    assertNotNull( row["male"]["3"] )
                    assertNotNull( row["male"]["2"] )
                    assertNotNull( row["male"]["1"] )

                    assertEquals( 146, row["male"]["3"].size() )
                    assertEquals( 63, row["male"]["2"].size() )
                    assertEquals( 57, row["male"]["1"].size() )

                    assertNotNull( row["female"] )
                    assertNotNull( row["female"]["3"] )
                    assertNotNull( row["female"]["2"] )
                    assertNotNull( row["female"]["1"] )

                    assertEquals( 72, row["female"]["3"].size() )
                    assertEquals( 30, row["female"]["2"].size() )
                    assertEquals( 50, row["female"]["1"].size() )
                    return row
                }
            }
            .go()
    }

    @Test
    void testInnerJoin() {
        int rejections = 0
        LoadStatistic stats = from(GratumFixture.people).join( from(GratumFixture.hobbies), ['id'] )
            .addStep("Assert hobbies") { Map row ->
                assertNotNull( row.hobby )
                return row
            }
            .onRejection { Pipeline pipeline ->
                pipeline.addStep("Verify rejection") { Map row ->
                    rejections++
                    return row
                }
                return
            }
            .go()

        assertEquals( 8, stats.loaded )
        assertEquals( 1, stats.rejections )
        assertEquals( 1, rejections )
    }

    @Test
    void testLeftJoin() {
        LoadStatistic stats = from(GratumFixture.people).join( from(GratumFixture.hobbies), ['id'], true )
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
    void testSort() {
        String lastHobby
        from(GratumFixture.hobbies).sort("hobby").addStep("Assert order is increasing") { Map row ->
            if( lastHobby ) assertTrue( "Assert ${lastHobby} < ${row.hobby}", lastHobby.compareTo( row.hobby ) <= 0 )
            lastHobby = row.hobby
            return row
        }.go()

        assertNotNull("Assert that lastHobby is not null meaning we executing some portion of the assertions above.", lastHobby)
    }

    @Test
    void testUnique() {
        LoadStatistic stats = from(GratumFixture.hobbies).unique("id")
        .go()

        assertEquals( 4, stats.loaded )
        assertEquals( 4, stats.rejections )
        assertEquals( 4, stats.getRejections(RejectionCategory.IGNORE_ROW) )
    }

    @Test
    void testRejections() {
        List<Map> rejections = []

        LoadStatistic stats = from(GratumFixture.hobbies)
                .addStep("Rejection") { Map row -> row.id > 1 ? row : reject( row,"${row.id} is too small", RejectionCategory.REJECTION) }
                .onRejection { Pipeline pipeline ->
                    pipeline.addStep("Save rejections") { Map row ->
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

    @Test(timeout = 5000L)
    public void testHttp() {
        String message = null
        int actualCount = 0
        int expectedCount = 0
        LoadStatistic stats = http("http://api.open-notify.org/astros.json").get()
            .inject { Map json ->
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
    void testAsDate() {
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
    void testAsInt() {
        LoadStatistic stats = from([
                [name: 'Chuck', atBats: '200', hits: '100'],
                [name: 'Sam', atBats: '300', hits: '125'],
                [name: 'Rob', atBats: '100', hits: '75'],
                [name: 'Sean', atBats: '20', hits: 'none']
        ]).asInt('hits')
        .go()

        assert 3 == stats.loaded
        assert 1 == stats.rejections
        assert 1 == stats.getRejections(RejectionCategory.INVALID_FORMAT)
    }

    @Test
    void testAsDouble() {
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
    void testAsBoolean() {
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
            assert row.member instanceof Boolean
            assert row.member
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
            assert row.member != true
            return row
        }.go()

    }

    @Test
    void testSort2() {
        Integer last = -1
        LoadStatistic stats = from( GratumFixture.people )
                .filter([gender: 'male'])
                .sort('age')
                .addStep('ageN > age1') {Map row ->
                    assert row.age >= last
                    last = (Integer)row.age
                    return row
                }
                .go()

        assertEquals( 2, stats.loaded )
        assertEquals( 3, stats.rejections )
        assertEquals( GratumFixture.people.size(), stats.loaded + stats.rejections )
    }

    @Test
    void testHeaderless() {
        LoadStatistic stats = csv("src/test/resources/headerless.csv", "|", ["Date", "status", "client", "server", "url", "length", "thread", "userAgent", "referer"])
            .addStep("Assert Columns Exist") { Map row->
                assertNotNull( row.status )
                assertNotNull( row.Date )
                assertNotNull( row.client )
                assertNotNull( row.server )
                assert !row.Date.isEmpty()
                assert !row.client.isEmpty()
                assert !row.server.isEmpty()
                assertEquals(9, row.size())
                return row
            }
            .trim()
            .go()

        assertEquals(18, stats.loaded)
        assertEquals( 0, stats.rejections )
    }

    @Test
    void testClip() {
        LoadStatistic stat = from(GratumFixture.people).clip("name", "gender").addStep("Test resulting rows") { Map row ->
            assertEquals( 2, row.size() )
            assertTrue( row.containsKey("name") )
            assertTrue( row.containsKey("gender") )
            return row
        }.go()

        assertEquals( 5, stat.loaded )
        assertEquals( 0, stat.rejections )
    }

    @Test
    void testRagged() {
        LoadStatistic stats = csv("src/test/resources/ragged.csv", ",")
            .addStep("Assert Row") { Map row ->
                assert row.containsKey("assignment")
                switch(row.rank) {
                    case "Captain":
                        assert row.assignment == null
                        break
                    case "Private":
                        assert row.assignment == "Old River"
                        break
                    case "Sergeant":
                        assert row.assignment == "Lombok"
                        break
                    case "Private First Class":
                        assert row.assignment == null
                        break
                }
                return row
            }.go()

        assertEquals( 4, stats.loaded )
        assertEquals( 0, stats.rejections )
    }

    @Test
    void testSave() {
        File tmp = File.createTempFile("people", ".csv")
        try {
            LoadStatistic stat = from(GratumFixture.people)
                    .save(tmp.absolutePath, "|")
                    .addStep("Verify we have a CSV file on the Pipeline") { Map row ->
                        assert row.file != null
                        assert row.filename == tmp.absolutePath
                        assert row.stream != null
                        assert row.stream instanceof Openable
                        return row
                    }
                    .go()
            assert stat.loaded == GratumFixture.people.size()
            assert stat.rejections == 0

            csv(tmp.absolutePath, "|")
                .addStep("assert 5 columns") { Map row ->
                    assert row.size() == 5
                    assert row["comment"].contains("\n")
                    return row
                }.go()
        } finally {
            tmp.delete()
        }
    }

    @Test
    public void testEscaping() {
        LoadStatistic stats = csv("src/test/resources/ragged.csv", ",")
            .addStep("Test Escaping") { Map row ->
                switch(row.rank) {
                    case "Captain":
                        assert row.comment.contains("\n")
                        assert row.comment == "This is a comment\nwith new lines"
                        break
                    case "Private":
                        assert row.comment.contains("\n")
                        assert row.comment == "This is another comment\nwith new lines"
                        break
                    case "Sergeant":
                        assert row.comment.startsWith('"')
                        assert row.comment.endsWith('"')
                        assert row.comment == '"This one has quotes"'
                        break
                    case "Private First Class":
                        assert row.comment == "More comments\nwith newlines\nin them"
                        break
                }
                return row
            }.go()
    }

    @Test
    public void testDefaultValues() {
        LoadStatistic stats = csv("src/test/resources/ragged.csv", ",")
            .addStep("Assert null exists") { Map row ->
                if( ['Captain', 'Private First Class'].contains( row.rank ) ) {
                    assert row.assignment == null
                }
                return row
            }
            .defaultValues([
                    assignment: 'None'
            ])
            .addStep("Assert defaults") { Map row ->
                assert row.assignment != null
                switch( row.rank ) {
                    case 'Captain':
                        assert row.assignment == "None"
                        break
                    case 'Private First Class':
                        assert row.assignment == "None"
                        break
                    default:
                        assert row.assignment != "None"
                        break
                }
                return  row
            }
            .go()
    }

    @Test
    public void testDefaultsBy() {
        from([
                [name: 'State 1', tax: null, defaultTax: 4.5],
                [name: 'State 2', tax: 5.5, defaultTax: 4.5],
                [name: 'State 3', tax: null, defaultTax: 3.0 ]
        ])
        .addStep("Assert null exists") { Map row ->
            if( row.name != 'State 2' ) {
                assert row.tax == null
            }
            return row
        }
        .defaultsBy([tax: 'defaultTax'])
        .addStep("Assert non null tax") { Map row ->
            assert row.tax != null
            assert row.tax instanceof Number
            switch( row.name ) {
                case 'State 2':
                    assert row.tax > 5.0
                    break
                default:
                    assert row.tax >= 3.0 && row.tax < 5.0
            }
            return row
        }
        .go()
    }

    @Test
    public void testLimit() {
        LoadStatistic stat = from( GratumFixture.people ).limit(3).go()

        assert stat.loaded == 3
        assert stat.rejections == 0
    }

    @Test
    public void testLimitWithoutHalt() {
        LoadStatistic stat = from(GratumFixture.people).limit(3, false).go()

        assert stat.loaded == 3
        assert stat.rejections == 2
        assert stat.getRejections(RejectionCategory.IGNORE_ROW) == 2
    }

    @Test
    public void testProcessingHeader() {
        boolean headerCallback = false
        LoadStatistic stats = CsvSource.of("src/test/resources/titanic.csv").header { List<String> headers ->
            headerCallback = true
            assert headers.size() == 11
        }.into().limit(0).go()

        assert stats.loaded == 0
        assert stats.rejections == 0
        assert headerCallback
    }

    @Test
    public void testCsvWithoutEscaping() {
        int line = 1
        CsvSource.of("src/test/resources/unescaped.csv", "|")
                .escaping(false)
                .into()
                .trim()
                .addStep("Test csv without escaping") { Map row ->
                    switch( line ) {
                        case 1:
                            assert row.ConNameFirst == "martini"
                            break
                        case 2:
                            assert row.ConNameFirst == 'La\"Quint'
                            break
                        case 3:
                            assert row.ConNameLast == 'o"neill'
                            break
                    }
                    assert row.size() == 55
                    line++
                    return row
                }
                .go()
    }

    @Test
    void testRejectionsFilterAcrossMultiplePipelines() {
        try {
            boolean rejectionsCalled = false
            CollectionSource.of(GratumFixture.getPeople()).into()
                    .filter(gender: "male")
                    .save("testRejectionsFilterAcrossMultiplePipelines.csv", "|")
                    .onRejection { Pipeline rejections ->
                        rejections.addStep("Assert We see males") { Map row ->
                            rejectionsCalled = true
                            assert row.gender == "female"
                            return row
                        }
                        return
                    }
                    .go()

            assert rejectionsCalled
        } finally {
            File f = new File("testRejectionsFilterAcrossMultiplePipelines.csv")
            f.delete()
        }
    }

    @Test
    void testExchange() {
        LoadStatistic stats = from([
                [ product: "IDW - TEQ-77", features: [
                        [ name: 'doors', value: 2 ],
                        [ name: 'configuration', value: 'chest' ],
                        [ name: 'refrigerant', value: 'R-600a'],
                        [ name: 'Energy Use', value: 0.26],
                        [ name: 'Volume', value: 2.3]
                    ]
                ],
                [ product: 'IDW - RCM-VISID', features: [
                        [ name: 'doors', value: 1],
                        [ name: 'configuration', value: 'chest'],
                        [ name: 'refrigerant', value: 'R-600a'],
                        [ name: 'Energy Use', value: 0.29],
                        [ name: 'Volume', value: 2.3]
                    ]
                ],
                [ product: 'IDW - TEQ2', features: [
                        [ name: 'doors', value: 1],
                        [ name: 'configuration', value: 'vertical'],
                        [ name: 'refrigerant', value: 'R-600a'],
                        [ name: 'Energy Use', value: 0.31],
                        [ name: 'Volume', value: 2.27]
                    ]
                ],
                [ product: 'True Refrigeration - TMC-34-DS-S-SS-HC', features: [
                        [ name: 'doors', value: 2],
                        [ name: 'configuration', value: 'Chest'],
                        [ name: 'refrigerant', value: 'R-290'],
                        [ name: 'Energy Use', value: 0.72],
                        [ name: 'Volume', value: 11.06]
                    ]
                ]
            ]).exchange { Map row ->
                return from((Collection<Map>)row.features).setField("product", row.product )
            }
            .addStep("Check the rows and files") { Map row ->
                assert row.containsKey("product")
                assert row.containsKey("name")
                assert row.containsKey("value")
                assert row.product != null
                assert row.name != null
                assert row.value != null
                return row
            }
            .go()

        assert stats.loaded == 20
        assert stats.rejections == 0
    }

    @Test
    void testDoneCallbacksInTimings() {
        LoadStatistic stat = from(GratumFixture.hobbies).sort("hobby").go()

        assert stat.stepTimings.containsKey("${stat.name}.after".toString())
        assert stat.stepTimings["${stat.name}.after".toString()] > 0
    }
}
