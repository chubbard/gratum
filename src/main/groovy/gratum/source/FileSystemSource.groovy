package gratum.source

import gratum.etl.Pipeline

class FileSystemSource implements Source {

    File file
    FileFilter filter
    int line = 1

    List<InputStream> streams = []

    @Override
    void start(Pipeline pipeline) {
        pipeline.after {
            streams.each { InputStream stream -> stream.close() }
            return
        }
        process( file, pipeline )
    }

    void process(File file, Pipeline pipeline) {
        if( file.isDirectory() ) {
            File[] files = file.listFiles( filter )
            for( File f : files ) {
                process( f, pipeline )
            }
        } else {
            streams << file.newInputStream()
            pipeline.process([file: file, stream: streams.last()], line++)
        }
    }
}
