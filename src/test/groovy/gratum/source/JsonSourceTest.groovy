package gratum.source

import gratum.etl.LoadStatistic
import org.junit.Test

class JsonSourceTest {

    @Test
    void jsonSourceTest() {
        LoadStatistic stat = JsonSource.json("""[
            {"firstName": "Bob", "lastName": "Smith", "age": 41},
            {"firstName": "Don", "lastName": "Johnson", "age": 64},
            {"firstName": "Rick", "lastName": "Richards", "age": 72},
            {"firstName": "Frank", "lastName": "Kilgore", "age": 22}
        ]""").into()
            .addStep("assert names") { Map row ->
                assert ["Bob", "Don", "Rick", "Frank" ].contains( row.firstName )
                return row
            }
            .go()
        assert stat.loaded == 4
        assert stat.rejections == 0
    }

    @Test
    void jsonSourceWithPathTest() {
        LoadStatistic stat = JsonSource.json("""{
            "status": 200,
            "items": [
                { "name": "Lamp", "price": 30.00, "color": ["white", "black", "red"] },
                { "name": "Desk", "price": 90.00, "color": ["oak", "pine", "white", "brushed nickel"]  },
                { "name": "Chair", "price": 125.00, "color": ["gray", "black"] }
            ] 
        }""", ["items"]).into()
            .addStep("Assert colors") { Map row ->
                assert row["_root_json"]
                assert row.color?.size() > 0
                return row
            }
            .go()

        assert stat.loaded == 3
        assert stat.rejections == 0
    }
}
