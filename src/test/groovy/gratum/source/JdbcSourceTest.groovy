package gratum.source

import gratum.etl.LoadStatistic
import gratum.test.H2DatabaseExtension
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

class JdbcSourceTest {

    @RegisterExtension
    final H2DatabaseExtension h2Extension = new H2DatabaseExtension()
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
            .addData("Passengers", [
                    [PassengerId: 1, Pclass: 1, Name: "Martha Hensley", Age: 31, Sex: "F", SibSp: 73, Parch: 28, Ticket: "123", Fare: 25.30, Cabin: "401", Embarked: "P"],
                    [PassengerId: 2, Pclass: 1, Name: "Michael Hensley", Age: 32, Sex: "M", SibSp: 73, Parch: 28, Ticket: "124", Fare: 25.30, Cabin: "401", Embarked: "P"],
                    [PassengerId: 3, Pclass: 2, Name: "Shawn O'Malley", Age: 24, Sex: "M", SibSp: 3, Parch: 11, Ticket: "125", Fare: 20.30, Cabin: "402", Embarked: "F"],
                    [PassengerId: 4, Pclass: 2, Name: "Robert Macquarie", Age: 42, Sex: "M", SibSp: 4, Parch: 11, Ticket: "126", Fare: 20.30, Cabin: "403", Embarked: "F"],
                    [PassengerId: 5, Pclass: 3, Name: "Michelle Macguinn", Age: 22, Sex: "F", SibSp: 5, Parch: 10, Ticket: "127", Fare: 10.50, Cabin: "601", Embarked: "P"],
                    [PassengerId: 6, Pclass: 3, Name: "James Macquire", Age: 23, Sex: "M", SibSp: 3, Parch: 12, Ticket: "128", Fare: 10.50, Cabin: "602", Embarked: "F"],
                    [PassengerId: 7, Pclass: 2, Name: "Sarah Tulle", Age: 33, Sex: "F", SibSp: 5, Parch: 15, Ticket: "129", Fare: 20.30, Cabin: "202", Embarked: "P"],
                    [PassengerId: 8, Pclass: 2, Name: "Robert Tulle", Age: 35, Sex: "M", SibSp: 5, Parch: 15, Ticket: "130", Fare: 20.30, Cabin: "202", Embarked: "P"],
                    [PassengerId: 9, Pclass: 2, Name: "Michael Tulle", Age: 5, Sex: "M", SibSp: 5, Parch: 15, Ticket: "131", Fare: 5.30, Cabin: "202", Embarked: "P"],
                    [PassengerId: 10, Pclass: 2, Name: "Virginia Tulle", Age: 3, Sex: "F", SibSp: 5, Parch: 15, Ticket: "132", Fare: 5.30, Cabin: "202", Embarked: "P"]
            ])
    @Test
    void testJdbcSource() {
        def sql = h2Extension.getSql()
        int loaded = 0
        LoadStatistic stat = JdbcSource.database( sql ).query("select * from Passengers").into()
            .addStep("Count Rows") { row ->
                assert row["PassengerId"]
                loaded++
                return row
            }
            .go()

        assert stat.loaded == loaded
    }
}
