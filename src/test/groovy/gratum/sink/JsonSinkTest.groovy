package gratum.sink

import gratum.etl.LoadStatistic
import gratum.source.CollectionSource
import org.junit.Test


class JsonSinkTest {

    private Collection<Map<String,Object>> createBands() {
        return [
            [firstName: "Bob", lastName: "Dylan", instruments: ["vocals", "guitar", "harmonica"]],
            [firstName: "Mick", lastName: "Jagger", band: "Rolling Stones", instruments: ["vocals", "tamborine", "harmonica"]],
            [firstName: "Keith", lastName: "Richards", band: "Rolling Stones", instruments: ["guitar"]],
            [firstName: "Ronnie", lastName: "Wood", band: "Rolling Stones", instruments: ["bass guitar"]],
            [firstName: "Charlie", lastName: "Watts", band: "Rolling Stones", instruments: ["drums"]],
            [firstName: "Brian", lastName: "Jones", band: "Rolling Stones", instruments: ["saxophone"]],
            [firstName: "Ian", lastName: "Stewart", band: "Rolling Stones", instruments: ["piano"]],
            [firstName: "David", lastName: "Gahon", band: "Depeche Mode", instruments: ["vocals"]],
            [firstName: "Andy", lastName: "Fletcher", band: "Depeche Mode", instruments: ["synth"]],
            [firstName: "Martin", lastName: "Gore", band: "Depeche Mode", instruments: ["synth", "guitar"]]
        ]
    }

    @Test
    public void testJsonOutput() {
        File tmp = File.createTempFile("testJsonOutput", ".json")
        try {
            LoadStatistic stats = CollectionSource.from( createBands() )
            .json(tmp.absolutePath)
            .go()

            assert stats.loaded == 10
            assert stats.rejections == 0
            assert tmp.getText().split("\n").length == stats.loaded + 3 // [\n + 10 rows + \n]\n
        } finally {
            tmp.delete()
        }
    }

    @Test
    public void testJsonlOutput(){
        File tmp = File.createTempFile("testJsonOutput", ".json")
        try {
            LoadStatistic stats = CollectionSource.from( createBands() )
                    .jsonl(tmp)
                    .go()

            assert stats.loaded == 10
            assert stats.rejections == 0
            assert tmp.getText().split("\n").length == stats.loaded // 10 rows
        } finally {
            tmp.delete()
        }
    }
}
