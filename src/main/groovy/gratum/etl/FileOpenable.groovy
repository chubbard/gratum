package gratum.etl

/**
 * Represents a file that can be cast to an InputStream or OutputStream giving
 * it the ability to masquerade as an InputStream/OutputStream.  You can only
 * have one open at a time (ie either InputStream or OutputStream).
 *
 * The usage is something like this:
 *
 * <code>
 *     InputStream is = row.fileOpenable as InputStream
 *     ...
 * </code>
 *
 * It understands how to convert through as keyword to java.io.InputStream,
 * java.io.OutputStream, and java.io.File.
 *
 * This is mostly used by gratum to transport stream data as a file so it can
 * be read as a stream transparently through multiple steps on the Pipline without
 * concern for needing to centrally decorate or chain streams.
 */
class FileOpenable implements Openable, Closeable {

    private final File file
    private InputStream inStream
    private OutputStream outStream

    public FileOpenable( File f ) {
        this.file = f
    }

    @Override
    Object asType(Class clazz) {
        if( clazz == InputStream ) {
            if( outStream ) throw new IllegalStateException("Cannot be opened as InputStream because an OutputStream already exists!")
            if (!inStream) {
                inStream = file.newInputStream()
            }
            return inStream
        } else if( clazz == OutputStream ) {
            if (inStream) throw new IllegalStateException("Cannot be opened as OutputStream because an InputStream already exists!")
            if (!outStream) {
                outStream = file.newOutputStream()
            }
            return outStream
        } else if( clazz == File ) {
            return file
        } else {
            throw new ClassCastException("Cannot convert to " + clazz.getName() )
        }
    }

    @Override
    void close() throws IOException {
        outStream?.close()
        inStream?.close()
    }

    public <V> V withInputStream(Closure<V> closure) {
        if( outStream ) throw new IllegalStateException("Cannot be opened as InputStream because an OutputStream already exists!")
        return (V)file.withInputStream( closure )
    }

    public <V> V withOutputStream(Closure<V> closure) {
        if( inStream ) throw new IllegalStateException("Cannot be opened as OutputStream because an InputStream already exists!")
        return (V)file.withOutputStream( closure )
    }
}
