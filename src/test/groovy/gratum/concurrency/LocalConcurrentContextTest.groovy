package gratum.concurrency

import gratum.etl.GratumFixture
import gratum.etl.LoadStatistic
import org.junit.Before
import org.junit.Ignore
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
                    .apply( context.worker { pipeline ->
                        pipeline.addStep("Assert we are on another thread") { Map row ->
                            assert Thread.currentThread().name.startsWith("Worker")
                            return row
                        }
                    }
                    .results { pipeline ->
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
            LoadStatistic stats = csv("titantic", stream, ",")
                    .apply( context.worker { pipeline ->
                        pipeline.filter("Only Females") { Map row ->
                            row.Sex == "female"
                        }
                    }
                    .results { pipeline ->
                        pipeline.addStep("Assert we are on the results thread") { Map row ->
                            assert Thread.currentThread().name.startsWith("Results")
                            return row
                        }
                    }
                    .connect())
                    .go()

            assert stats.loaded == 152      // female
            assert stats.rejections == 266  // male
            assert stats.stepTimings.containsKey("Queue to Workers")
            assert stats.stepTimings.containsKey("Queue to Results")
            assert stats.stepTimings.containsKey("Assert we are on the results thread")
            assert stats.stepTimings.containsKey("Only Females")
        }
    }

}
