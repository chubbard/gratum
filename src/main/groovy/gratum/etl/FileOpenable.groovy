package gratum.etl

class FileOpenable implements Openable, AutoCloseable {

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
        } else {
            throw new ClassCastException("Cannot convert to " + clazz.getName() )
        }
    }

    @Override
    void close() throws Exception {
        outStream?.close()
        inStream?.close()
    }
}
