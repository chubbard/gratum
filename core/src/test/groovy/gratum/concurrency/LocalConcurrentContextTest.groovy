package gratum.concurrency

import gratum.etl.GratumFixture
import gratum.etl.LoadStatistic
import gratum.etl.Pipeline
import org.junit.Before
import org.junit.Test

import static gratum.source.CsvSource.csv

class LocalConcurrentContextTest {

    LocalConcurrentContext context

    @Before
    void setUp() {
        context = new LocalConcurrentContext(4, 50 )
    }

    @Test
    void testCompletion() {
        GratumFixture.getResource("titanic.csv").withStream {stream ->
            LoadStatistic stats = csv("titantic", stream, ",")
                    .apply( context.spread { pipeline ->
                        pipeline.addStep("Assert we are on another thread") { Map row ->
                            assert Thread.currentThread().name.startsWith("Worker")
                            return row
                        }
                    }
                    .collect { pipeline ->
                        pipeline.addStep("Assert we are on the results thread") { Map row ->
                            assert Thread.currentThread().name.startsWith("Results")
                            return row
                        }
                    }
                    .connect())
                    .go()

            assert stats.loaded == 418
            assert stats.rejections == 0
        }
    }

    @Test
    void testRejections() {
        GratumFixture.withResource("titanic.csv") { stream ->
            LoadStatistic stats = csv("titanic", stream, ",")
                    .apply( context.spread { pipeline ->
                        pipeline.filter("Only Females") { row ->
                            row.Sex == "female"
                        }
                    }
                    .collect { Pipeline pipeline ->
                        pipeline.addStep("Assert we are on the results thread") { row ->
                            assert Thread.currentThread().name.startsWith("Results")
                            return row
                        }
                    }
                    .connect())
                    .go()

            assert stats.loaded == 152      // female
            assert stats.rejections == 266  // male
            assert stats.contains("Queue to Workers")
            assert stats.contains("Queue to Results")
            assert stats.contains("Assert we are on the results thread")
            assert stats.contains("Only Females")
        }
    }

}
