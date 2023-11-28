package gratum.aws.sink

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.SendMessageResult
import gratum.etl.Pipeline
import gratum.sink.*
import groovy.json.JsonOutput

class SqsSink implements Sink {

    String queueUrl
    int count = 0

    private final AmazonSQS client

    public SqsSink(String queueUrl) {
        this.queueUrl = queueUrl
        client = AmazonSQSClientBuilder.defaultClient()
    }

    public static SqsSink queue(String queueUrl) {
        return new SqsSink(queueUrl)
    }

    @Override
    String getName() {
        return queueUrl;
    }

    @Override
    void attach(Pipeline pipeline) {
        pipeline.addStep("Post to SQS ${queueUrl}") { row ->
            String json = JsonOutput.toJson( row )
            SendMessageResult result = client.sendMessage(queueUrl, json )
            count++
            row
        }
    }

    @Override
    Map<String, Object> getResult() {
        return [queue: queueUrl, count: count ]
    }

    @Override
    void close() throws IOException {
        client.sendMessage(queueUrl, '{"_done":true}')
        client.shutdown()
    }
}
