package gratum.source

import gratum.etl.Pipeline
import gratum.util.UncloseableInputStream
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory

class ArchivedSource extends AbstractSource {

    File file
    String format

    ArchivedSource(File file) {
        this.name = file.name
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
            try {
                ArchiveEntry entry
                while ((entry = archive.getNextEntry()) != null) {
                    if (!entry.isDirectory() && archive.canReadEntryData(entry)) {
                        pipeline.process([filename: file.name, file: file, entry: entry, stream: new UncloseableInputStream(archive)], line++)
                    }
                }
            } finally {
                archive.close()
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
