package gratum.aws.sink

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.SendMessageBatchRequest
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry
import com.amazonaws.services.sqs.model.SendMessageBatchResult
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.amazonaws.services.sqs.model.SendMessageResult
import gratum.etl.Pipeline
import gratum.sink.*
import groovy.json.JsonOutput

class SqsSink implements Sink {

    private final String queueUrl
    private int count = 0
    private int batchSize = 1
    private SendMessageBatchRequest batchRequest

    private final AmazonSQS client

    public SqsSink(String queueUrl) {
        this.queueUrl = queueUrl
        client = AmazonSQSClientBuilder.defaultClient()
    }

    public static SqsSink queue(String queueUrl) {
        return new SqsSink(queueUrl)
    }

    public SqsSink batchSize(int size) {
        this.batchSize = size
        return this
    }

    @Override
    String getName() {
        return queueUrl;
    }

    @Override
    void attach(Pipeline pipeline) {
        pipeline.addStep("Post to SQS ${queueUrl}") { row ->
            String json = JsonOutput.toJson( row )
            if( batchSize > 1 ) {
                if( !batchRequest ) {
                    batchRequest = new SendMessageBatchRequest(queueUrl)
                }
                if( batchRequest.getEntries().size() < batchSize ) {
                    batchRequest.getEntries().add( new SendMessageBatchRequestEntry(
                            UUID.randomUUID().toString(),
                            json
                    ))
                } else {
                    SendMessageBatchResult res = client.sendMessageBatch( batchRequest )
                    batchRequest = null
                }
            } else {
                SendMessageResult result = client.sendMessage(queueUrl, json )
            }
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
        if( batchRequest?.entries ) {
            client.sendMessageBatch( batchRequest )
        }
        SendMessageRequest req = new SendMessageRequest(queueUrl, '{"_done":true}')
            .withDelaySeconds(30)
        client.sendMessage(req)
        client.shutdown()
    }
}
