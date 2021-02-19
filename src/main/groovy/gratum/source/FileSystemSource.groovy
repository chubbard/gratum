package gratum.source

import gratum.etl.Pipeline
import groovy.io.FileType

import java.util.regex.Pattern

/**
 * Creates a source that enumerates files within a directory and returns
 * a Pipeline that produces a Map object with keys file and value of
 * {@link java.io.File}.  It enumerates only Files (i.e. File.isFile() == true),
 * but recursively visits all subdirectories.  You can include an optional
 * filter to match files.
 */
class FileSystemSource extends AbstractSource {

    File file
    Pattern filter = ~/.*/
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

    FileSystemSource filter(Pattern filenameFilter ) {
        this.filter = filenameFilter
        return this
    }

    void process(File file, Pipeline pipeline) {
        file.eachFileRecurse(FileType.FILES) { File current ->
            if( current.name =~ filter ) {
                pipeline.process([file: current], line++)
            }
        }
    }
}
