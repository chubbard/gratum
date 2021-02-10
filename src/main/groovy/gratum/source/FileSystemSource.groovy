package gratum.source

import gratum.etl.Pipeline

/**
 * Creates a source that enumerates files within a directory and returns
 * a Pipeline that produces a Map object with keys file and value of
 * {@link java.io.File}.  It enumerates only Files (i.e. File.isFile() == true),
 * but recursively visits all subdirectories.  You can include an optional
 * filter to match files.
 */
class FileSystemSource extends AbstractSource {

    File file
    Object filter = ~/.*/
    int line = 1

    FileSystemSource(File file) {
        this.name = file.name
        this.file = file
    }

    static FileSystemSource files( File directory ) {
        return new FileSystemSource( directory )
    }

    @Override
    void start(Pipeline pipeline) {
        process( file, pipeline )
    }

    FileSystemSource filter( Object filenameFilter ) {
        this.filter = filenameFilter
        return this
    }

    void process(File file, Pipeline pipeline) {
        if( file.isDirectory() ) {
            file.eachFileMatch( filter ) { File current ->
                process( current, pipeline )
            }
        } else {
            pipeline.process([file: file], line++)
        }
    }
}
