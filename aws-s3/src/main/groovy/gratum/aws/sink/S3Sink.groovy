import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectResult
import com.amazonaws.services.s3.model.S3Object
import gratum.aws.source.S3Openable
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

    static S3Sink s3(String bucket, key, column, @DelegatesTo(S3Sink) Closure configure = null) {
        S3Sink sink = new S3Sink(s3Bucket: bucket, s3Key: key, column: column)
        if( configure ) {
            configure.delegate = sink
            configure.call()
        }
        return sink
    }

    @Override
    String getName() {
        return "s3://${s3Bucket}/${s3Key}"
    }

    @Override
    void attach(Pipeline pipeline) {
        pipeline.addStep("Upload(${s3Key})") { row ->
            if( row[column] instanceof File ) {
                PutObjectResult result = s3Client.putObject(s3Bucket, s3Key, (File)row[column])
            } else if( row[column] instanceof InputStream ) {
                ((InputStream)row[column]).withStream { stream ->
                    PutObjectResult result = s3Client.putObject(s3Bucket, s3Key, stream, metaData)
                }
            } else if( row[column] instanceof Openable ) {
                Openable openable = (Openable) row[column]
                (openable as InputStream).withStream { stream ->
                    PutObjectResult result = s3Client.putObject(s3Bucket, s3Key, stream, metaData)
                }
            } else if( row[column] ) {
                PutObjectResult result = s3Client.putObject( s3Bucket, s3Key, row[column].toString())
            }
            row
        }
    }

    @Override
    Source getResult() {
        S3Object s3Object = s3Client.getObject(s3Bucket, s3Key)
        return CollectionSource.of([
                s3Bucket: s3Bucket,
                s3Key: s3Key,
                s3: s3Object,
                stream: new S3Openable( s3Object )
        ])
    }

    @Override
    void close() throws IOException {
        s3Client.shutdown()
    }
}