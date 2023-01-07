package gratum.source

import gratum.etl.Pipeline
import gratum.util.UncloseableInputStream
import groovy.transform.CompileStatic
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory

/**
 * ArchivedSource supports other archive formats that the JDK doesn't support.  Things like Zip64 (ie Enhanced_Deflate),
 * sevenZ (7z), arj, etc.  This uses the apache common-compress library under the hood so any format it can read
 * should be available with this source.
 *
 * Each row holds the following keys:
 * <ul>
 *     <li>filename - Filename of the archive file.</li>
 *     <li>file - underlying archive java.io.File that this came from.</li>
 *     <li>entry - org.apache.commons.compress.archivers.ArchiveEntry representing a file within the archive</li>
 *     <li>stream - The java.io.InputStream of this entry in the archive</li>
 * </ul>
 *
 * Examples:
 * For Zip, Zip64, etc:
 * <code>
 * ArchivedSource.unzip( file ).into()
 * ...
 * .go()
 * </code>
 *
 * For 7z:
 * <code>
 * ArchivedSource.sevenZ( file ).into()
 * ...
 * .go()
 * </code>
 *
 * For auto-detect format:
 * <code>
 * ArchivedSource.unarchive( file ).into()
 * ...
 * .go()
 * </code>
 *
 * Specific format:
 * <code>
 *     ArchivedSource.unarchive( file ).format( ArchiveStreamFactory.ARJ ).into()
 *     ...
 *     .go()
 * </code>
 */
@CompileStatic
class ArchivedSource extends AbstractSource {

    File file
    String format

    ArchivedSource(File file) {
        super( file.name )
        this.file = file
    }

    public static ArchivedSource sevenZ( File file ) {
        return new ArchivedSource( file ).format( ArchiveStreamFactory.SEVEN_Z )
    }

    public static ArchivedSource unzip(File zip ) {
        return new ArchivedSource( zip ).format( ArchiveStreamFactory.ZIP )
    }

    public static ArchivedSource unarchive( File archive ) {
        return new ArchivedSource( archive )
    }

    public ArchivedSource format(String format) {
        this.format = format
        return this
    }

    @Override
    void doStart(Pipeline pipeline) {
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
