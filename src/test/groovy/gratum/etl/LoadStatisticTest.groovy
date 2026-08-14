package gratum.etl

import gratum.source.CollectionSource
import org.junit.jupiter.api.Test

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

        assert stat1.stepTimings.size() == 2
        assert stat2.stepTimings.size() == 3

        stat1.merge( stat2 )

        assert stat1.loaded == 3
        assert stat1.rejections == 4
        assert stat1.getRejections( RejectionCategory.IGNORE_ROW ) == 4
        assert stat1.getRejectionsFor( RejectionCategory.IGNORE_ROW ).size() == 3
        assert stat1.getRejectionsFor(RejectionCategory.IGNORE_ROW)["filter color -> [green, blue]"] == 1
        assert stat1.getRejectionsFor(RejectionCategory.IGNORE_ROW)["filter color -> [yellow, grey]"] == 2
        assert stat1.getRejectionsFor(RejectionCategory.IGNORE_ROW)["filter color -> grey"] == 1

        assert stat1.stepTimings.size() == 5
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

        assert stat1.stepTimings.size() == 2
        assert stat2.stepTimings.size() == 3

        stat1.merge( stat2, false )

        assert stat1.stepTimings.size() == 2
    }

    @Test
    void testMetadata() {
        LoadStatistic stat1 = CollectionSource.from([
                [color: 'red'],
                [color: 'green'],
                [color: 'blue'],
                [color: 'orange'],
                [color: 'white'],
                [color: 'black'],
                [color: 'yellow'],
                [color: 'purple'],
                [color: 'cyan']
        ]).filter([color: ['green', 'blue']])
        .addStep("wait") { row ->
            Thread.sleep(10L)
            return row
        }
                .go()

        stat1.addMetadata('executionOrder', 0).addMetadata('rejectionFilename', 'rejected-colors.csv')

        assert stat1.start < stat1.end
        assert stat1.duration.toMillis() > 0
        assert stat1.metadata['rejectionFilename'] == 'rejected-colors.csv'
        assert stat1.metadata['executionOrder'] == 0
        assert !stat1.metadata['rejected-colors.csv']
    }
}
