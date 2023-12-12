import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.S3Object
import gratum.aws.source.S3Openable
import gratum.etl.Pipeline
import gratum.source.AbstractSource

public class S3Source extends AbstractSource {

    private String s3Bucket
    private String s3Key

    private AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient()

    @Override
    void doStart(Pipeline pipeline) {
        try {
            S3Object s3Object = s3Client.getObject(s3Bucket, s3Key)
            pipeline.process(s3: s3Object, stream: new S3Openable(s3Object))
        } finally {
            s3Client.shutdown()
        }
    }
}