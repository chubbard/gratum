package gratum.etl

import gratum.sink.CollectionSink
import gratum.sink.Sink
import gratum.source.ClosureSource
import gratum.source.CollectionSource
import gratum.source.CsvSource
import gratum.source.Source
import org.junit.Test

import static junit.framework.TestCase.*
import static gratum.source.CsvSource.*
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
        assert statistic.duration.toMillis() < 100
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
            }
            .go()

        assert statistic
        assert statistic.name == "titanic.csv"
        assert statistic.loaded == 266
        assert statistic.rejections == 152
        assert statistic.getRejections(RejectionCategory.IGNORE_ROW) == 152
        assert statistic.duration.toMillis() < 100
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
        List<String> filter = ["3", "2"]

        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
                .filter([Pclass: filter, Sex: "male"])
                .onRejection { Pipeline rej ->
                    rej.addStep("verify not sex != male && pClass != 3") { Map row ->
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

    @Test
    void testFilterMap() {
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
            .filter([Pclass: "3", Sex: "male"])
            .onRejection { Pipeline rej ->
                rej.addStep("verify not sex != male && pClass != 3") { Map row ->
                    assert row.Pclass != "3" || row.Sex != "male"
                    return row
                }
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
            }
            .go()

        assert statistic.loaded == 200
        assert statistic.rejections == 218
        assert statistic.getRejections( RejectionCategory.IGNORE_ROW ) == 218
    }

    @Test
    void testWildcardFilterClosure() {
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
            .filter([ "*": { Map row -> row['Pclass'] == "3" || row['Sex'] == "Male"  } ])
            .onRejection { Pipeline rej ->
                rej.addStep("Verify all rejections Pclass <> 3 and Sex <> Male") { Map row ->
                    assert row.Pclass != "3" && row.Sex != "Male"
                    return row
                }
            }
            .go()

        assert statistic.loaded == 218
        assert statistic.rejections == 200
        assert statistic.getRejections(RejectionCategory.IGNORE_ROW) == 200
    }

    @Test
    void testSimpleGroupBy() {
        LoadStatistic statistic = csv("src/test/resources/titanic.csv")
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
        assert statistic.contains( "asInt(Age)")
        assert statistic.contains("filter()")
        assert statistic.contains("groupBy(Sex,Pclass)")
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
        assert statistic.loaded == 1
        assert statistic.rejections == 418

        assertTrue( "Assert that the timings include the filter(Sex->K)) step", statistic.contains("filter Sex -> K") )
        // I'm not entirely sure why this assert was added.  It seems logical to include
        // it as it's a step on the Pipeline but it was done for specific reasons, but
        // those reasons are lost to history.  I'm leaving it in case I remember why this was
        // important or not.
//        assertFalse( "Assert that timings does NOT include groupBy because all rows are filtered out.", statistic.stepTimings.containsKey("groupBy(Sex)") )
        assert statistic.contains("groupBy(Sex)")
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

        assert stats.loaded == 4
        assert stats.rejections == 0
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
        boolean branchEntered = false, branchAfter = false
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
                    .after {
                        branchAfter = true
                        return
                    }
            }
            .addStep("Verify branch field is NOT on the outer Pipeline") { Map row ->
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
        assert branchAfter
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

    @Test
    void testComplexBranch() {
        List<Map<String,Object>> rows = []
        boolean closedCalled = false
        csv("src/test/resources/titanic.csv")
            .branch("Testing complex branching") { pipeline ->
                pipeline.inject { row ->
                    return [ row ]
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
                    Source getResult() {
                        CollectionSource.of( [ rows: rows ] )
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
                    pipeline.addStep("Save rejections") { Map row ->
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
        .addStep("Assert all are Dates") { Map row ->
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

        assert stats.loaded == 2
        assert stats.rejections == 3
        assert GratumFixture.people.size() == stats.loaded + stats.rejections
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
        LoadStatistic stat = from(GratumFixture.people)
                .clip("name", "gender")
                .addStep("Test resulting rows") { Map row ->
            assert row.size() == 2
            assertTrue( row.containsKey("name") )
            assertTrue( row.containsKey("gender") )
            return row
        }.go()

        assert stat.loaded == 5
        assert stat.rejections == 0
    }

    @Test
    void testClipWithCollection() {
        LoadStatistic stat = from(GratumFixture.people)
                .clip(["name", "gender"])
                .addStep("Test resulting rows") { Map row ->
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
            .addStep("Assert Row") { Map row ->
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
                    assert !row.assignment
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
        boolean invoked = false
        LoadStatistic stat = from(GratumFixture.hobbies).filter(hobby: ~/T.*/).after {
            invoked = true
            Thread.sleep(10) // ensure we don't go too fast :-)
        }.go()

        assert invoked : "after callback was NOT called!"
        assert stat.doneStatistics.find {it.name == "${stat.name}.0.after".toString() && it.duration > 0 }
        assert stat.doneStatistics.find {it.name == "${stat.name}.0.after".toString() && it.duration > 0 }.duration > 0
//        println( stat.toString(true) )
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
            .addStep("Test for date") { Map row ->
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
                .addStep("Assert") { Map row ->
                    assert ['blue', 'red', 'green'].contains(row.color_code)
                    return row
                }
                .go()
        assert stat.loaded == 3
        assert stat.rejections == 0
    }

    @Test
    public void testEmptyToNull() {
        LoadStatistic stat = from(
                [
                        red: "",
                        green: "1.0",
                        blue: ""
                ],
                [
                        red: "",
                        green: "0.5",
                        blue: "0.5"
                ],
                [
                        red: "1.0",
                        green: "",
                        blue: "0.5"
                ],
                [
                        red: "0.5",
                        green: "0.5",
                        blue: ""
                ],
                [
                        red: "",
                        green: 0,
                        blue: "1.0"
                ]
        )
        .addStep("Assert all strings") { Map row ->
            assert row.keySet().inject(0) {x, s -> row[s] instanceof String && row[s].isEmpty() ? x + 1 : x } >= 1
            return row
        }
        .emptyToNull()
        .filter("Remove anything without a null") { Map row ->
            row.keySet().inject (0) { x, s -> row[s] == null ? x + 1 : x } >= 1
        }
        .go()

        assert stat.loaded == 5
        assert stat.rejections == 0
    }

    @Test
    public void testClosureSourceExchange() {
        File tmp = File.createTempDir()
        PipelineTest.getResourceAsStream("/people.csv").withStream { stream ->
            new File( tmp, "people.csv").withOutputStream { os ->
                os << stream
            }
        }
        File buildDir = new File( tmp, "build" )
        buildDir.mkdirs()

        List<File> files = [
                new File( tmp, "people.csv")
        ]
        new ClosureSource( { Pipeline pipeline ->
            files.each { File f ->
                pipeline.process( [file: f])
            }
            return
        }).into()
        .exchange { Map row ->
            File input = row.file as File
            Map<String,String> mappings = [ name: "George Burdell" ]
            return csv(input, ",")
                    .apply { Pipeline pipeline ->
                        mappings?.each { pipeline.setField(it.key,it.value) }
                        return pipeline
                    }
                    .save( new File( buildDir, input.name ).absolutePath, ",")
        }
        .go()

        File tmpPeople = new File( buildDir, "people.csv" )

        assert tmpPeople.exists()
        assert tmpPeople.length() > 0
    }

    @Test
    void testFormat() {
        CollectionSource.from([
                [
                        name: "Buck",
                        daysActive: 1024,
                        date: new Date(2016-1900,3,21)
                ],
                [
                        name: "Rogers",
                        daysActive: 10504,
                        date: new Date(2011-1900, 6, 21)
                ]
        ])
        .format("%1\$tY-%1\$tm-%1\$td", "date")
        .format("%,d", "age")
        .addStep("assert format worked") { row ->
            assert row["date"] instanceof String
            assert row["age"] instanceof String
            row
        }
        .save(new CollectionSink("result",[]))
        .addStep("test formatting") { row ->
            assert row["result"]
            row
        }
        .go()

    }

    @Test
    void testDateFormat() {
        CollectionSource.from([
                [
                        date: new Date( 2020-1900, 5, 25 )
                ],
                [
                        date: new Date(1969-1900, 7, 16)
                ],
                [
                        date: new Date(1492-1900, 4, 28)
                ]
        ])
        .dateFormat("yyyy-MM-dd", "date")
        .addStep("Verify date is a String") { row ->
            assert row["date"] instanceof String
            assert row["date"] ==~ /\d{4}-\d{2}-\d{2}/
            row
        }
        .go()
    }
}
