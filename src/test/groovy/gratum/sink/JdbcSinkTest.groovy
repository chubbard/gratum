package gratum.sink

import gratum.etl.GratumFixture
import gratum.etl.LoadStatistic
import gratum.source.CsvSource
import gratum.test.H2DatabaseExtension
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

class JdbcSinkTest {

    @RegisterExtension
    final H2DatabaseExtension h2 = new H2DatabaseExtension()
            .addTable("Passengers", [
                    PassengerId: Integer,
                    Pclass: Integer,
                    Name: String,
                    Age: Double,
                    Sex: String,
                    SibSp: Integer,
                    Parch: Integer,
                    Ticket: String,
                    Fare: Double,
                    Cabin: String,
                    Embarked: String
            ])

    @Test
    void testJdbcSink() {
        def sql = h2.getSql()

        LoadStatistic stat = GratumFixture.getResource("titanic.csv").withCloseable { stream ->
            CsvSource.of("titanic.csv", stream, ",").into()
                .emptyToNull()
                .asDouble("Age")
                .asDouble("Fare")
                .asInt("PassengerId")
                .asInt("Pclass")
                .asInt("SibSp")
                .asInt("Parch")
                .save( JdbcSink.batch( sql, "Passengers", [
                        "PassengerId",
                        "Pclass",
                        "Name",
                        "Sex",
                        "Age",
                        "SibSp",
                        "Parch",
                        "Ticket",
                        "Fare",
                        "Cabin",
                        "Embarked"
                ]) )
            .go()
        }

        def results = sql.firstRow("select count(*) as count from Passengers")
        int count = results.count as int
        assert count == stat.loaded
    }
}
