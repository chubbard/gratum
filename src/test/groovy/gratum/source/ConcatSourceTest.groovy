package gratum.source

import gratum.etl.LoadStatistic

import static gratum.source.ConcatSource.*
import static gratum.source.CollectionSource.*

import org.junit.Test

class ConcatSourceTest {

    List<Map> people1 = [
            [id: 1, name: 'Bill Rhodes', age: 53, gender: 'male'],
            [id: 2, name: 'Cheryl Lipscome', age: 43, gender: 'female'],
            [id: 3, name: 'Diana Rogers', age: 34, gender: 'female'],
            [id: 4, name: 'Jack Lowland', age: 25, gender: 'male'],
            [id: 5, name: 'Ginger Rogers', age: 83, gender: 'female']
    ]

    List<Map> people2 = [
            [id: 6, name: 'Charles Randall', age: 32, gender: 'male'],
            [id: 7, name: 'Will Hurt', age: 19, gender: 'male'],
            [id: 8, name: 'Lisa Cortez', age: 23, gender: 'female'],
            [id: 9, name: 'Juan Lopez', age: 29, gender: 'male'],
            [id: 10, name: 'Carl Douglas', age: 64, gender: 'male']
    ]


    @Test
    public void testConcatSource() {
        Set ids = [] as Set
        LoadStatistic stat = concat(of(people1), of(people2) ).into()
            .addStep("Collect IDs") { Map row ->
                ids << row.id
                return row
            }
            .go()

        assert stat.loaded == 10
        assert stat.rejections == 0
        assert ids.size() == 10
    }
}
