package gratum.source

import gratum.etl.Pipeline
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory

class ArchivedSource extends AbstractSource {

    File file
    String format

    ArchivedSource(File file) {
        this.file = file
    }

    public static ArchivedSource sevenZ( File file ) {
        return new ArchivedSource( file ).format( ArchiveStreamFactory.SEVEN_Z )
    }

    public static ArchivedSource unzip(File zip ) {
        return new ArchivedSource( zip ).format( ArchiveStreamFactory.ZIP )
    }

    public ArchivedSource format(String format) {
        this.format = format
        return this
    }

    @Override
    void start(Pipeline pipeline) {
        int line = 1
        this.file.withInputStream { InputStream stream ->
            ArchiveInputStream archive = getArchiveInputStream(stream)
            ArchiveEntry entry = archive.getNextEntry()
            while( entry ) {
                if( !entry.isDirectory() && archive.canReadEntryData(entry) ) {
                    pipeline.process( [filename: file.name, file: file, entry: entry, stream: archive], line++ )
                    entry = archive.getNextEntry()
                }
            }
        }
    }

    private ArchiveInputStream getArchiveInputStream(InputStream stream) {
        if( format ) {
            return new ArchiveStreamFactory().createArchiveInputStream(format, stream)
        } else {
            return new ArchiveStreamFactory().createArchiveInputStream(stream)
        }
    }
}
