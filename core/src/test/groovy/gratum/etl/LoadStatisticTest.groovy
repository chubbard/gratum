package gratum.etl

import gratum.source.CollectionSource
import org.junit.Test

class LoadStatisticTest {

    @Test
    public void testMerge() {
        LoadStatistic stat1 = CollectionSource.from([
                [color: 'red'],
                [color: 'green'],
                [color: 'blue']
        ]).filter([color: ['green', 'blue']])
                .go()

        LoadStatistic stat2= CollectionSource.from([
                [color: 'yellow'],
                [color: 'brown'],
                [color: 'black'],
                [color: 'grey']
        ]).filter([color: ['yellow', 'grey']])
                .filter([color: 'grey'])
                .go()

        assert stat1.loaded == 2
        assert stat1.rejections == 1
        assert stat1.getRejectionsFor(RejectionCategory.IGNORE_ROW).size() == 1
        assert stat1.getRejectionsFor(RejectionCategory.IGNORE_ROW)["filter color -> [green, blue]"] == 1

        assert stat2.loaded == 1
        assert stat2.rejections == 3

        assert stat2.getRejectionsFor(RejectionCategory.IGNORE_ROW).size() == 2
        assert stat2.getRejectionsFor(RejectionCategory.IGNORE_ROW)["filter color -> [yellow, grey]"] == 2
        assert stat2.getRejectionsFor(RejectionCategory.IGNORE_ROW)["filter color -> grey"] == 1

        assert stat1.stepStatistics.size() == 1
        assert stat2.stepStatistics.size() == 2

        stat1.merge( stat2 )

        assert stat1.loaded == 3
        assert stat1.rejections == 4
        assert stat1.getRejections( RejectionCategory.IGNORE_ROW ) == 4
        assert stat1.getRejectionsFor( RejectionCategory.IGNORE_ROW ).size() == 3
        assert stat1.getRejectionsFor(RejectionCategory.IGNORE_ROW)["filter color -> [green, blue]"] == 1
        assert stat1.getRejectionsFor(RejectionCategory.IGNORE_ROW)["filter color -> [yellow, grey]"] == 2
        assert stat1.getRejectionsFor(RejectionCategory.IGNORE_ROW)["filter color -> grey"] == 1

        assert stat1.stepStatistics.size() == 3
    }

    @Test
    public void testMergeWithoutStepTimings() {
        LoadStatistic stat1 = CollectionSource.from([
                [color: 'red'],
                [color: 'green'],
                [color: 'blue']
        ]).filter([color: ['green', 'blue']])
                .go()

        LoadStatistic stat2= CollectionSource.from([
                [color: 'yellow'],
                [color: 'brown'],
                [color: 'black'],
                [color: 'grey']
        ]).filter([color: ['yellow', 'grey']])
                .filter([color: 'grey'])
                .go()

        assert stat1.stepStatistics.size() == 1
        assert stat2.stepStatistics.size() == 2

        stat1.merge( stat2   )

        assert stat1.stepStatistics.size() == 3
    }
}
