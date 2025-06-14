package gratum.etl

import gratum.csv.CSVFile
import gratum.sink.Sink
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
        GratumFixture.withResource("titanic.csv") { stream ->
            int afterRows = 0, beforeRows = 0
            LoadStatistic statistic = csv("titanic.csv", stream)
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
            assert statistic.duration.toMillis() < 100
        }
    }

    @Test
    void testSimpleFilter() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic statistic = csv("titanic.csv", stream)
                .filter([Sex:"male"])
                .onRejection { Pipeline rej ->
                    rej.addStep("Verify sex was filtered out") { Map row ->
                        assertFalse( row.Sex == "male")
                        return row
                    }
                    return
                }
                .go()

            assert statistic
            assert statistic.name == "titanic.csv"
            assert statistic.loaded == 266
            assert statistic.rejections == 152
            assert statistic.getRejections(RejectionCategory.IGNORE_ROW) == 152
            assert statistic.duration.toMillis() < 100
        }
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

        assert statistic
        assert statistic.name == "titanic.csv"
        assert statistic.loaded == 185
        assert statistic.rejections == 233
        assert statistic.getRejections(RejectionCategory.IGNORE_ROW) == 233
        assert statistic.getRejectionsFor(RejectionCategory.IGNORE_ROW).size() == 1
        assert statistic.getRejectionsFor(RejectionCategory.IGNORE_ROW).keySet().first()
        assert statistic.getRejections(RejectionCategory.IGNORE_ROW,"filter()") == 233
    }

    @Test
    void testFilterMapWithCollection() {
        GratumFixture.withResource("titanic.csv") { stream ->
            List<String> filter = ["3", "2"]
            LoadStatistic statistic = csv( "titanic.csv", stream )
                .filter([Pclass: filter, Sex: "male"])
                .onRejection { rej ->
                    rej.addStep("verify not sex != male && pClass != 3") { row ->
                        assert !filter.contains(row.Pclass) || row.Sex != "male"
                        return row
                    }
                }
                .go()

            assert statistic.loaded == 209
            assert statistic.rejections == 209
            assert statistic.getRejections(RejectionCategory.IGNORE_ROW) == 209
            assert statistic.getRejections( RejectionCategory.IGNORE_ROW, "filter Pclass -> [3, 2],Sex -> male" ) == 209
        }
    }

    @Test
    void testFilterMap() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic statistic = csv( "titanic.csv", stream )
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
    }

    @Test
    void testFilterMapWithClosure() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic statistic = csv("titanic.csv", stream )
                    .filter([Pclass: { value -> (value as Integer) < 3 } ])
                    .onRejection { Pipeline rej ->
                        rej.addStep("Verify all rejections are == 3") { Map row ->
                            assert row.Pclass == "3"
                            return row
                        }
                    }
                    .go()

            assert statistic.loaded == 200
            assert statistic.rejections == 218
            assert statistic.getRejections( RejectionCategory.IGNORE_ROW ) == 218
        }
    }

    @Test
    void testWildcardFilterClosure() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic statistic = csv("titanic.csv", stream)
                    .filter([ "*": { Map row -> row['Pclass'] == "3" || row['Sex'] == "Male"  } ])
                    .onRejection { Pipeline rej ->
                        rej.addStep("Verify all rejections Pclass <> 3 and Sex <> Male") { Map row ->
                            assert row.Pclass != "3" && row.Sex != "Male"
                            return row
                        }
                        return
                    }
                    .go()

            assert statistic.loaded == 218
            assert statistic.rejections == 200
            assert statistic.getRejections(RejectionCategory.IGNORE_ROW) == 200
        }
    }

    @Test
    void testSimpleGroupBy() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic statistic = csv("titanic.csv", stream)
                    .groupBy("Sex")
                    .addStep("Assert groupBy(Sex)") { Map row ->
                        assert row.male?.size() == 266
                        assert row.female?.size() == 152
                        return row
                    }
                    .go()
            assert statistic.loaded == 1
            assert statistic.rejections == 0
        }
    }

    @Test
    void testMultipleGroupBy() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic statistic = csv("titanic.csv", stream)
                    .asInt("Age")
                    .filter() { Map row ->
                        row.Age ? row.Age < 20 : false
                    }
                    .groupBy("Sex", "Pclass")
                    .addStep("Assert groupBy(Sex,Pclass") { Map row ->
                        // size reflects how many sub-values there are for each group.
                        // not total items fitting into this group, only
                        // the leaves hold that information.
                        assert row.male?.size() == 3
                        assert row.female?.size() == 3

                        assert row.male['1'].size() == 3
                        assert row.male['2'].size() == 7
                        assert row.male['3'].size() == 16

                        assert row.female['1'].size() == 2
                        assert row.female['2'].size() == 7
                        assert row.female['3'].size() == 16

                        return row
                    }
                    .go()
            // verify that step timings for steps before the groupBy() are included in the returned statistics
            assert statistic.stepTimings.containsKey("asInt(Age)")
            assert statistic.stepTimings.containsKey("filter()")
            assert statistic.stepTimings.containsKey("groupBy(Sex,Pclass)")
        }
    }

    @Test
    void testEmptyGroupBy() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic statistic = csv("titanic.csv", stream)
                    .filter([Sex: 'K'])
                    .groupBy("Sex")
                    .addStep("Assert groupBy(Sex)") { row ->
                        assertTrue(row.isEmpty())
                        return row
                    }
                    .go()
            assert statistic.loaded == 1
            assert statistic.rejections == 418

            assertTrue("Assert that the timings include the filter(Sex->K)) step", statistic.stepTimings.containsKey("filter Sex -> K"))
            // I'm not entirely sure why this assert was added.  It seems logical to include
            // it as it's a step on the Pipeline but it was done for specific reasons, but
            // those reasons are lost to history.  I'm leaving it in case I remember why this was
            // important or not.
            //        assertFalse( "Assert that timings does NOT include groupBy because all rows are filtered out.", statistic.stepTimings.containsKey("groupBy(Sex)") )
            assert statistic.stepTimings.containsKey("groupBy(Sex)")
        }
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
        ])
        .filter("Filter out Sam") { row ->
            row['name'] != 'Sam'
        }
        .concat( from([
                [name: 'Rob', atBats: '100', hits: '75', battingAverage: '0.75'],
                [name: 'Sean', atBats: '20', hits: 'none', battingAverage: 'none'],
                [name: 'Sam', atBats: '350', hits: '127', battingAverage: '0.363']
        ]))
        .addStep("Assert we get rows") { row ->
            called++
            if( row.name == 'Sam' ) {
                assert row.atBats == '350'
            }
            return row
        }
        .go()

        assert stats.loaded == 4
        assert stats.rejections == 1
        assert stats.loaded == called
    }

    @Test
    void testTrim() {
        LoadStatistic stats = from([
                [firstName: 'Roger   ', lastName: '  Smith'],
                [firstName: '    Jill', lastName: 'Belcher'],
                [firstName: '\tRick\t', lastName: 'Spade   ']
        ])
        .trim()
        .addStep("Assert all values were trimmed") { row ->
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
                .addStep("Assert completed is defined") { row ->
                    assert row.completed
                    return row
                }
                .go()

        assert stats.loaded == 5
    }

    @Test
    void renameFields() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic statistic = csv("titanic.csv", stream)
                .addStep("Test Sex Exists") { row ->
                    assertTrue("Assert row.Sex exists", row.containsKey("Sex"))
                    assertTrue("Assert row.Age exists", row.containsKey("Age"))
                    return row
                }
                .renameFields([Sex: "gender", "Age": "age"])
                .addStep("Test Sex renamed to gender and Age to age") { row ->
                    assertTrue(row.containsKey("gender"))
                    assertTrue(row.containsKey("age"))
                    return row
                }
                .go()
        }
    }

    @Test
    void testAddField() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic statistic = csv("titanic.csv", stream)
                .addField("survived") { row ->
                    return true
                }
                .addStep("Test Field added") { row ->
                    assertTrue( row.containsKey("survived") )
                    return row
                }
                .go()
        }
    }

    @Test
    void testFillDownBy() {
        GratumFixture.withResource("fill_down.csv") { stream ->
            int count = 0
            LoadStatistic statistic = csv("fill_down.csv", stream)
                    .addStep("Assert fields are missing data.") { row ->
                        if (!row.first_name) {
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
                    .addStep("Assert values were filled down") { row ->
                        row.each { String key, Object value ->
                            assertNotNull("Assert ${key} is filled in with a value", value)
                            assertTrue("Assert that ${key} is non-empty", !(value as String).isEmpty())
                        }
                        return row
                    }
                    .go()

            assertTrue("Assert that we encountered rows that weren't filled in", count > 0)
        }
    }

    @Test
    void testBranch() {
        boolean branchEntered = false
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic statistic = csv("titanic.csv", stream)
                .branch { Pipeline pipeline ->
                    return pipeline.filter([Sex: "female"])
                            .addStep("Verify sex was filtered out") { row ->
                                assertTrue(row.Sex == "female")
                                return row
                            }
                            .defaultValues("branch": true)
                            .addStep("Branch Entered") { Map row ->
                                assert row.branch
                                branchEntered = true
                                return row
                            }
                }
                .addStep("Verify branch field is NOT on the outer Pipeline") { row ->
                    assert row.branch == null
                    return row
                }
                .filter([Sex: "male"])
                .addStep("Verify sex was filtered to male") { row ->
                    assertTrue(row.Sex == "male")
                    return row
                }
                .go()
            assert branchEntered
        }
    }

    @Test
    void testBranchWithGroupBy() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic statistic = csv("titanic.csv", stream)
                .branch { Pipeline p ->
                    return p.groupBy("Sex", "Pclass").addStep { row ->
                        assertNotNull(row["male"])
                        assertNotNull(row["male"]["3"])
                        assertNotNull(row["male"]["2"])
                        assertNotNull(row["male"]["1"])

                        assert row["male"]["3"].size() == 146
                        assert row["male"]["2"].size() == 63
                        assert row["male"]["1"].size() == 57

                        assert row["female"]
                        assert row["female"]["3"]
                        assert row["female"]["2"]
                        assert row["female"]["1"]

                        assert row["female"]["3"].size() == 72
                        assert row["female"]["2"].size() == 30
                        assert row["female"]["1"].size() == 50
                        return row
                    }
                }
                .go()
        }
    }

    @Test
    void testComplexBranch() {
        GratumFixture.withResource("titanic.csv") { stream ->
            List<Map<String,Object>> rows = []
            boolean closedCalled = false
            LoadStatistic statistic = csv("titanic.csv", stream)
                    .branch("Testing complex branching") { pipeline ->
                        pipeline.inject { row ->
                            return [row]
                        }
                        .save(new Sink() {
                            @Override
                            String getName() {
                                return "collection"
                            }

                            @Override
                            void attach(Pipeline p) {
                                p.addStep("Collect") { row ->
                                    rows << row
                                    return row
                                }
                            }

                            @Override
                            Map<String, Object> getResult() {
                                return [rows: rows]
                            }

                            @Override
                            void close() throws IOException {
                                closedCalled = true
                            }
                        })
                    }
                    .go()

            assert rows.size() > 0
            assert closedCalled
        }
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
                pipeline.addStep("Verify rejection") { row ->
                    rejections++
                    return row
                }
                return
            }
            .go()

        assert stats.loaded == 8
        assert stats.rejections == 1
        assert rejections == 1
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

        assert stats.loaded == 9
        assert stats.rejections == 0
    }

    @Test
    void testSort() {
        String lastHobby
        from(GratumFixture.hobbies).sort("hobby").addStep("Assert order is increasing") { row ->
            if( lastHobby ) assertTrue( "Assert ${lastHobby} < ${row.hobby}", lastHobby.compareTo( row.hobby ) <= 0 )
            lastHobby = row.hobby
            return row
        }.go()

        assertNotNull("Assert that lastHobby is not null meaning we executing some portion of the assertions above.", lastHobby)
    }

    @Test
    void testSortOrder() {
        String lastHobby
        from(GratumFixture.hobbies)
                .sort(new Tuple2<>("hobby", SortOrder.DESC))
                .addStep("Assert order is increasing") { row ->
                    if( lastHobby ) assertTrue( "Assert ${lastHobby} > ${row.hobby}", lastHobby.compareTo( row.hobby ) >= 0 )
                    lastHobby = row.hobby
                    return row
                }.go()

        assertNotNull("Assert that lastHobby is not null meaning we executing some portion of the assertions above.", lastHobby)
    }

    @Test
    void testSortExternalWithoutDownstream() {
        List<Map<String,Object>> hobbies = []
        from((0..10_000).collect { r ->
            GratumFixture.hobbies[r%GratumFixture.hobbies.size()]
        }).sort("Sort Hobby") {
            pageSize = 1000
            downstream = false
            comparator = new Comparator<Map<String, Object>>() {
                @Override
                int compare(Map<String, Object> o1, Map<String, Object> o2) {
                    if( o1 && !o2 ) return -1
                    if( !o1 && o2 ) return 1
                    String hobby1 = o1["hobby"]
                    String hobby2 = o2["hobby"]
                    return hobby1 <=> hobby2
                }
            }
            after { file ->
                assert file != null
                assert file.size() > 0
                CSVFile csv = new CSVFile( file, "," );
                try {
                    csv.withCloseable {
                        String lastHobby = ""
                        for (Map<String, Object> row : csv.mapIterator()) {
                            String hobby = row["hobby"]
                            assert (lastHobby <=> hobby) <= 0
                            lastHobby = hobby
                        }
                    }
                } finally {
                    file.delete()
                }
            }
        }
        .addStep("Assert Downstream isn't called") { row ->
            fail("Downstream steps have been turned off.")
            return row
        }.go()
    }

    @Test
    void testUnique() {
        LoadStatistic stats = from(GratumFixture.hobbies).unique("id")
        .go()

        assert stats.loaded == 4
        assert stats.rejections == 4
        assert stats.getRejections(RejectionCategory.IGNORE_ROW) == 4
    }

    @Test
    void testRejections() {
        List<Map> rejections = []

        LoadStatistic stats = from(GratumFixture.hobbies)
                .addStep("Rejection") { Map row -> row.id > 1 ? row : reject( row,"${row.id} is too small", RejectionCategory.REJECTION) }
                .onRejection { Pipeline pipeline ->
                    pipeline.addStep("Save rejections") { row ->
                        rejections << row
                        return row
                    }
                    return null
                }
                .go()

        assert stats.loaded == 6
        assert stats.rejections == 2
        assert stats.getRejections(RejectionCategory.REJECTION) == 2
        assert rejections.size() == 2
    }

    @Test(timeout = 15000L)
    public void testHttpSource() {
        String message = null
        int actualCount = 0
        int expectedCount = 0
        LoadStatistic stats = http("http://api.open-notify.org/astros.json").get()
            .inject { json ->
                expectedCount = json.number
                message = json.message
                json.people
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

    @Test
    void testAsDate() {
        boolean kirby = false
        LoadStatistic stats = from([
                [name: 'Chuck', dateOfBirth: '1992-08-11'],
                [name: 'Sam', dateOfBirth: '1980-04-12'],
                [name: 'Rob', dateOfBirth: 'unknown'],
                [name: 'Sean' ],
                [name: 'Kirby', dateOfBirth: new Date()],
                [name: 'Huck', dateOfBirth: '08/12/1994']
        ]).asDate('dateOfBirth', 'yyyy-MM-dd', 'MM/dd/yyyy')
        .addStep("Assert all are Dates") { row ->
            if( row['name'] == 'Kirby' ) {
                kirby = true
            }
            if( row.containsKey('dateOfBirth') ) {
                assert row['dateOfBirth'] instanceof Date
            }
            row
        }
        .go()

        assert stats.loaded == 5
        assert stats.rejections == 1
        assert stats.getRejections(RejectionCategory.INVALID_FORMAT) == 1
        assert kirby
    }

    @Test
    void testAsDateFormat() {
        LoadStatistic stats = from([
                [name: 'Chuck', dateOfBirth: '08/11/1992'],
                [name: 'Sam', dateOfBirth: '1980-04-12'],
                [name: 'Rob', dateOfBirth: '07/17/1980'],
        ]).asDate('dateOfBirth', 'MM/dd/yyyy')
        .asDate('dateOfBirth')  // make sure it doesn't do anything if it's already a date.
        .go()

        assert stats.loaded == 2
        assert stats.rejections == 1
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

        assert stats.loaded == 3
        assert stats.rejections == 1
        assert stats.getRejections(RejectionCategory.INVALID_FORMAT) == 1
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
        .addStep("Assert all are boolean true") { row ->
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
        .addStep("Assert all are boolean true") { row ->
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
                .addStep('ageN > age1') {row ->
                    assert row.age >= last
                    last = (Integer)row.age
                    return row
                }
                .go()

        assert stats.loaded == 2
        assert stats.rejections == 3
        assert GratumFixture.people.size() == stats.loaded + stats.rejections
    }

    @Test
    void testHeaderless() {
        LoadStatistic stats = csv("src/test/resources/headerless.csv", "|", ["Date", "status", "client", "server", "url", "length", "thread", "userAgent", "referer"])
            .addStep("Assert Columns Exist") { row->
                assertNotNull( row.status )
                assertNotNull( row.Date )
                assertNotNull( row.client )
                assertNotNull( row.server )
                assert !row.Date.isEmpty()
                assert !row.client.isEmpty()
                assert !row.server.isEmpty()
                assert row.size() == 9
                return row
            }
            .trim()
            .go()

        assert stats.loaded == 18
        assert stats.rejections == 0
    }

    @Test
    void testClip() {
        LoadStatistic stat = from(GratumFixture.people).clip("name", "gender").addStep("Test resulting rows") { row ->
            assert row.size() == 2
            assertTrue( row.containsKey("name") )
            assertTrue( row.containsKey("gender") )
            return row
        }.go()

        assert stat.loaded == 5
        assert stat.rejections == 0
    }

    @Test
    void testRagged() {
        LoadStatistic stats = csv("src/test/resources/ragged.csv", ",")
            .addStep("Assert Row") { row ->
                assert row.containsKey("assignment")
                switch(row.rank) {
                    case "Captain":
                        assert !row.assignment
                        break
                    case "Private":
                        assert row.assignment == "Old River"
                        break
                    case "Sergeant":
                        assert row.assignment == "Lombok"
                        break
                    case "Private First Class":
                        assert !row.assignment
                        break
                }
                return row
            }.go()

        assert stats.loaded == 4
        assert stats.rejections == 0
    }

    @Test
    void testSave() {
        File tmp = File.createTempFile("people", ".csv")
        try {
            LoadStatistic stat = from(GratumFixture.people)
                    .save(tmp, "|")
                    .addStep("Verify we have a CSV file on the Pipeline") { row ->
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
                .addStep("assert 5 columns") { row ->
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
            .addStep("Test Escaping") { row ->
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
            .addStep("Assert null exists") { row ->
                if( ['Captain', 'Private First Class'].contains( row.rank ) ) {
                    assert !row.assignment
                }
                return row
            }
            .defaultValues([
                    assignment: 'None'
            ])
            .addStep("Assert defaults") { row ->
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
        .addStep("Assert null exists") { row ->
            if( row.name != 'State 2' ) {
                assert row.tax == null
            }
            return row
        }
        .defaultsBy([tax: 'defaultTax'])
        .addStep("Assert non null tax") { row ->
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
        GratumFixture.withResource("titanic.csv") { stream ->
            boolean headerCallback = false
            LoadStatistic stats = CsvSource.of("titanic.csv", stream).header { List<String> headers ->
                headerCallback = true
                assert headers.size() == 11
            }.into().limit(0).go()

            assert stats.loaded == 0
            assert stats.rejections == 0
            assert headerCallback
        }
    }

    @Test
    public void testCsvWithoutEscaping() {
        int line = 1
        GratumFixture.withResource("unescaped.csv") { stream ->
            LoadStatistic statistic = CsvSource.of("unescaped.csv", stream, "|")
                .escaping(false)
                .into()
                .trim()
                .addStep("Test csv without escaping") { row ->
                    switch (line) {
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
    }

    @Test
    void testRejectionsFilterAcrossMultiplePipelines() {
        try {
            boolean rejectionsCalled = false
            CollectionSource.of(GratumFixture.getPeople()).into()
                    .filter(gender: "male")
                    .save("testRejectionsFilterAcrossMultiplePipelines.csv", "|")
                    .onRejection { rejections ->
                        rejections.addStep("Assert We see only females") { row ->
                            rejectionsCalled = true
                            assert row.gender == "female"
                            return row
                        }
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
            ]).exchange { row ->
                return from((Collection<Map>)row.features).setField("product", row.product )
            }
            .addStep("Check the rows and files") { row ->
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
    void testExchangeRejections() {
        GratumFixture.withResource("titanic.csv", { stream ->
            LoadStatistic stats = csv("titanic.csv", stream, ",")
                .filter(Sex: "female")  // make sure rejections before the exchange are counted in the final result
                .exchange { row ->
                    String ticket = row.Ticket
                    List<Map<String,Object>> ticketNumbers = []
                    for( int i = 0; i < ticket.length(); i++ ) {
                        if( ticket[i].isInteger() ) {
                            int number = ticket[i] as int
                            ticketNumbers << [number: number]
                        }
                    }
                    return CollectionSource.from( ticketNumbers )
                }
                .addStep("Are Ticket Numbers <= 5") { row ->
                    // make sure rejections after the exchange are counted
                    row.number <= 5 ? row : reject(row, "Ticket Number > 5", RejectionCategory.REJECTION)
                }
                .go()

            assert stats.loaded == 549
            assert stats.getRejections(RejectionCategory.IGNORE_ROW) == 266
            assert stats.getRejections(RejectionCategory.REJECTION) == stats.rejections - stats.getRejections(RejectionCategory.IGNORE_ROW)
        })
    }

    @Test
    void testDoneCallbacksInTimings() {
        boolean called = false
        long s = System.currentTimeMillis()
        LoadStatistic stat = from(GratumFixture.hobbies).sort("hobby").after {
            called = true
            Thread.sleep(10) // ensure we don't go too fast :-)
            long e = System.currentTimeMillis()
            LoadStatistic afterStat = toLoadStatistic(s,e)
            assert afterStat.loaded == 8
        }.go()

        assert stat.stepTimings.containsKey("sort([hobby]).after")
        assert called
        assert stat.stepTimings["sort([hobby]).after"] > 0
    }

    @Test
    void testNameOverride() {
        LoadStatistic stat = CsvSource.of("src/test/resources/titanic.csv").name("bill_and_ted_do_the_titanic").into().go()
        assert stat.name == "bill_and_ted_do_the_titanic"
    }

    @Test
    void testReplaceAll() {
        LoadStatistic stat = from([date: '(Tue) 12/12/1999'], [date: '(Thu) 9/17/2001'], [date:'(Wed) 3/1/2022'])
            .replaceAll("date", ~/\(\w+\)/, "")
            .addStep("Test for date") { row ->
                assert ["(Tue)", "(Wed)", "(Thu)"].findAll() {r -> !row.date.contains(r) }.size() == 3
                row
            }
            .go()
        assert stat.loaded == 3
        assert stat.rejections == 0
    }

    @Test
    void testReplaceValues() {
        LoadStatistic stat = from([color_code: 1], [color_code: 2], [color_code: 3])
                .replaceValues("color_code", [
                        '1':'blue',
                        '2':'red',
                        '3':'green'
                ])
                .addStep("Assert") { row ->
                    assert ['blue', 'red', 'green'].contains(row.color_code)
                    return row
                }
                .go()
        assert stat.loaded == 3
        assert stat.rejections == 0
    }

    @Test
    void testInject() {
        LoadStatistic stat = CollectionSource.from(GratumFixture.people)
            .filter { it.gender == 'female' }
            .inject { row ->
                GratumFixture.hobbies.findAll { hobby -> hobby.id == row.id }
            }
            .addStep("Mark these rows with a different rejection category") {
                it.id == 2 ? it : reject(it, "Not id == 2", RejectionCategory.REJECTION)
            }
            .go()
        assert stat.loaded == 2
        assert stat.rejections == 4
        // make sure we get rejections above the inject
        assert stat.getRejections(RejectionCategory.IGNORE_ROW) == 2
        // make sure we get rejections below the inject
        assert stat.getRejections(RejectionCategory.REJECTION) == 2
    }

    @Test
    void testReduce() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic stats = csv("titanic.csv", stream, ",")
                .asDouble("Fare")
                .reduce("Calculate Total", [ticketPrice:0.0d, count: 0]) { sum, row ->
                    Double fare = row["Fare"] ?: null
                    if( fare > 0.0d ) {
                        sum.ticketPrice += fare
                        sum.count++
                    }
                    return sum
                }
                .addStep("Test Reduce") { row ->
                    assert row.ticketPrice > 0.0d
                    assert row.count > 0
                    return row
                }
                .go()

            assert stats.loaded == 418
            assert stats.rejections == 0
        }
    }
}
