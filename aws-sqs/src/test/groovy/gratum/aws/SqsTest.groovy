package gratum.aws

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.CreateQueueRequest
import com.amazonaws.services.sqs.model.CreateQueueResult
import gratum.aws.sink.SqsSink
import gratum.aws.source.SqsSource
import gratum.etl.LoadStatistic
import gratum.source.CollectionSource
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future

//@Ignore("Requires AWS SDK credentials setup")
class SqsTest {

    public static final String SQS_TEST_QUEUE = "SqsTest_testSqs_${System.currentTimeMillis()}"

    static AmazonSQS sqs
    static CreateQueueResult queueResult
    static ExecutorService service

    @BeforeClass
    static void setup() {
        service = Executors.newFixedThreadPool(2)
        sqs = AmazonSQSClientBuilder.defaultClient()
        CreateQueueRequest req = new CreateQueueRequest(SQS_TEST_QUEUE)
//                .withAttributes(FifoQueue:"true")
        queueResult = sqs.createQueue(req)
    }

    @AfterClass
    static void tearDown() {
        sqs.deleteQueue(SQS_TEST_QUEUE)
        sqs.shutdown()
        service.shutdown()
    }

    @Test
    public void testSqs() {
        Future<LoadStatistic> sink = service.submit({
            CollectionSource.from([
                    id: 1,
                    name: "Chuck"
            ],[
                    id: 2,
                    name: "Rob"
            ],[
                    id: 3,
                    name: "Dark haired kid"
            ]).save(SqsSink.queue(queueResult.queueUrl))
                    .addStep("Test that we send 3 rows") { row ->
                        assert row["queue"] == queueResult.queueUrl
                        assert row["count"] == 3
                        row
                    }
                    .go()
        } as Callable<LoadStatistic>)

        Future<LoadStatistic> source = service.submit({
            SqsSource.queue(queueResult.queueUrl).into()
                    .addStep("Test we got things we expect") { row ->
                        assert row["id"]
                        assert row["name"]
                        row
                    }
                    .go()
        } as Callable<LoadStatistic>)

        LoadStatistic statSink = sink.get()
        LoadStatistic sourceSink = source.get()

        assert statSink.rejections == 0
        assert sourceSink.rejections == 0
        assert statSink.loaded == sourceSink.loaded
        println(statSink)
        println(sourceSink)
    }

    @Test
    public void testBatchedSqs() {
        Future<LoadStatistic> sink = service.submit({
            List messages = (1.50).collect { id -> [ id: id, name: "Name-${id}"] }
            CollectionSource.from(messages)
                    .save(SqsSink.queue(queueResult.queueUrl).batchSize(10))
                    .addStep("Test that we send 50 rows") { row ->
                        assert row["queue"] == queueResult.queueUrl
                        assert row["count"] == 50
                        row
                    }
                    .go()
        } as Callable<LoadStatistic>)

        Future<LoadStatistic> source = service.submit({
            SqsSource.queue(queueResult.queueUrl).into()
                    .addStep("Test we got things we expect") { row ->
                        assert row["id"]
                        assert row["name"]
                        row
                    }
                    .go()
        } as Callable<LoadStatistic>)

        LoadStatistic statSink = sink.get()
        LoadStatistic sourceSink = source.get()

        assert statSink.rejections == 0
        assert sourceSink.rejections == 0
        assert statSink.loaded == sourceSink.loaded
        println(statSink)
        println(sourceSink)
    }
}
