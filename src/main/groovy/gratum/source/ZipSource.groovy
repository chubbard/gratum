package gratum.source

import gratum.etl.Pipeline

import java.util.function.Consumer
import java.util.zip.ZipEntry
import java.util.zip.ZipFile

class ZipSource implements Source {

    File file

    ZipSource(String path) {
        this.file = new File( path )
    }

    ZipSource(File zip) {
        this.file = zip
    }

    public static Pipeline unzip(File zip ) {
        Pipeline pipeline = new Pipeline( zip.name )
        pipeline.src = new ZipSource( zip )
        return pipeline
    }

    @Override
    void start(Pipeline pipeline) {
        int line = 1
        ZipFile zip = new ZipFile( file )
        zip.stream().forEach( new Consumer<ZipEntry>() {
            @Override
            void accept(ZipEntry zipEntry) {
                pipeline.process( [entry: zipEntry, stream: zip.getInputStream(zipEntry)], line++ )
            }
        })
    }
}
