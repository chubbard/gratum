package gratum.source

import gratum.etl.FileOpenable
import gratum.etl.Pipeline
import groovy.io.FileType

import java.util.regex.Pattern

/**
 * Creates a source that enumerates files within a directory, the file itself,
 * and returns a Pipeline that produces a Map object with keys file and stream
 * with types {@link java.io.File} and {@link InputStream} respectively.  It
 * enumerates only Files (i.e. File.isFile() == true).  By default it recursively
 * visits all subdirectories {@link FileSystemSource#recursive(boolean) }.  You
 * can include an optional filter to match files
 * {@link FileSystemSource#filter(java.util.regex.Pattern)}.
 */
class FileSystemSource extends AbstractSource {

    File[] files
    Pattern filter = ~/.*/
    int line = 1
    boolean recursive = true

    FileSystemSource(File[] files) {
        this.name = files.collect {it.name }.join(",")
        this.files = files
    }

    /**
     * Pass any number of files to this method and this source will visit each
     * file.
     * @param directories 1 or more java.io.File objects to visit
     * @return this
     */
    static FileSystemSource files( File... directories ) {
        return new FileSystemSource( directories )
    }

    @Override
    void start(Pipeline pipeline) {
        for( File f : files ) {
            process( f, pipeline )
        }
    }

    /**
     * Add a filename filter using a regular expression to match filenames against.
     * Anything that matches the given filternameFilter will be passed onto the
     * Pipeline.
     * @param filenameFilter Regular expression used to match the file.
     * @return this
     */
    FileSystemSource filter(Pattern filenameFilter ) {
        this.filter = filenameFilter
        return this
    }

    /**
     * Configures this source to be visit directories recursively or not.
     * @param r true if you want to visit all directories recursively or
     * false if only the direct children
     * @return this
     */
    FileSystemSource recursive( boolean r ) {
        this.recursive = r
        return this
    }

    void process(File file, Pipeline pipeline) {
        if( file.isFile() ) {
            if( file.name =~ filter ) {
                pipeline.process([file: file, stream: new FileOpenable(file)], line++)
            }
        } else {
            if( recursive ) {
                file.eachFileRecurse(FileType.FILES) { File current ->
                    process( current, pipeline )
                }
            } else {
                file.eachFile( FileType.FILES ) { File current ->
                    process( current, pipeline )
                }
            }
        }
    }
}
