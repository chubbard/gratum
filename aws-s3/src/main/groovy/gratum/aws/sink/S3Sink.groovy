import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectResult
import gratum.etl.Openable
import gratum.etl.Pipeline
import gratum.sink.Sink
import gratum.source.CollectionSource
import gratum.source.Source

public class S3Sink implements Sink {

    private String s3Bucket
    private String s3Key
    private String column

    private ObjectMetadata metaData = new ObjectMetadata()
    private AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient()

    @Override
    String getName() {
        return "s3://${s3Bucket}/${s3Key}"
    }

    @Override
    void attach(Pipeline pipeline) {
        pipeline.addStep("Upload(${s3Key})") { row ->
            Openable openable = (Openable)row[column]
            (openable as InputStream).withStream { stream ->
                PutObjectResult result = s3Client.putObject(s3Bucket, s3Key, stream, metaData)
            }
            row
        }
    }

    @Override
    Source getResult() {
        return CollectionSource.of([
                s3Bucket: s3Bucket,
                s3Key: s3Key
        ])
    }

    @Override
    void close() throws IOException {
        s3Client.shutdown()
    }
}