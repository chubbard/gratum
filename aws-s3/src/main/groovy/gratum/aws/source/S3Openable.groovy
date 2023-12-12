package gratum.aws.source

import com.amazonaws.services.s3.model.S3Object
import gratum.etl.Openable

class S3Openable implements Openable, Closeable {

    S3Object s3Object
    InputStream inStream

    public S3Openable( S3Object obj ) {
        this.s3Object = obj
    }

    @Override
    Object asType(Class clazz) {
        if( clazz instanceof InputStream ) {
            inStream = s3Object.objectContent
        } else {
            throw new ClassCastException("Cannot convert to " + clazz.getName() )
        }
    }

    @Override
    void close() throws IOException {
        inStream?.close()
        s3Object.close()
    }
}
