package gratum.source

import gratum.etl.Pipeline

import java.util.function.Consumer
import java.util.zip.ZipEntry
import java.util.zip.ZipFile

class ZipSource extends AbstractSource {

    File file

    ZipSource(String path) {
        this( new File( path ) )
    }

    ZipSource(File zip) {
        this.name = zip.name
        this.file = zip
    }

    public static ZipSource unzip(File zip ) {
        return new ZipSource( zip )
    }

    @Override
    void start(Pipeline pipeline) {
        int line = 1
        ZipFile zip = new ZipFile( file )
        zip.stream().forEach( new Consumer<ZipEntry>() {
            @Override
            void accept(ZipEntry zipEntry) {
                pipeline.process( [filename: zip.name, file: zip, entry: zipEntry, stream: zip.getInputStream(zipEntry)], line++ )
            }
        })
    }
}
