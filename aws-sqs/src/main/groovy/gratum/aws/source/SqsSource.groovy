package gratum.aws.source

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.ReceiveMessageResult
import gratum.etl.Pipeline
import gratum.source.AbstractSource
import groovy.json.JsonSlurper

class SqsSource extends AbstractSource {

    private final String queueUrl
    private final AmazonSQS client
    private final JsonSlurper slurper = new JsonSlurper()

    SqsSource(String queueUrl) {
        super( queueUrl )
        this.queueUrl = queueUrl
        this.client = AmazonSQSClientBuilder.defaultClient()
    }

    public static SqsSource queue(String queueUrl) {
        return new SqsSource( queueUrl );
    }

    @Override
    void doStart(Pipeline pipeline) {
        int count = 0
        boolean done = false
        while( !done ) {
            ReceiveMessageResult result = client.receiveMessage( queueUrl )
            result.messages.each { message ->
                Map<String,Object> row = slurper.parseText(message.body)
                if( row["_done"] ) {
                    done = true
                } else {
                    pipeline.process( row, count )
                    count++
                }
                client.deleteMessage( queueUrl, message.receiptHandle)
            }
        }
        pipeline.finished()
    }
}
