package gratum.source

import gratum.etl.Pipeline
import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey
import org.apache.poi.poifs.filesystem.POIFSFileSystem

class XlsSource extends AbstractSource {

    POIFSFileSystem pfs
    int startOnRow = 0
    boolean outputFormulaValues = true
    String sheet = null

    XlsSource(String name, POIFSFileSystem pfs, String sheet = null) {
        this.name = name
        this.pfs = pfs
        this.sheet = sheet
    }

    public static XlsSource xls(String name, InputStream is, String sheet = null) {
        return new XlsSource( name, new POIFSFileSystem(is), sheet )
    }

    public static XlsSource xls(File file, String sheet = null) {
        return new XlsSource( file.name, new POIFSFileSystem(file, true), sheet )
    }

    XlsSource startOnRow( int row ) {
        this.startOnRow = row
        return this
    }

    XlsSource password(String password) {
        Biff8EncryptionKey.setCurrentUserPassword(password)
        return this
    }

    XlsSource outputFormulaValues( boolean output ) {
        this.outputFormulaValues = output
        return this
    }

    @Override
    void start(Pipeline pipeline) {
        try {
            XlsProcessor processor = new XlsProcessor( pipeline, startOnRow, outputFormulaValues, sheet )
            processor.parse( pfs )
        } finally {
            Biff8EncryptionKey.setCurrentUserPassword(null)
        }
    }
}
