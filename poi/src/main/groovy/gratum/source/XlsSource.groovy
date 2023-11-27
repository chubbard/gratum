package gratum.source

import gratum.etl.Pipeline
import groovy.transform.CompileStatic
import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey
import org.apache.poi.poifs.filesystem.POIFSFileSystem

/**
 * A {@link gratum.source.AbstractSource} that implements reading excel workbooks in xls
 * format also known as HSSF.
 */
@CompileStatic
class XlsSource extends AbstractSource {

    POIFSFileSystem pfs
    int startOnRow = 0
    boolean outputFormulaValues = true
    String sheet = null

    XlsSource(String name, POIFSFileSystem pfs, String sheet = null) {
        super(name)
        this.pfs = pfs
        this.sheet = sheet
    }

    /**
     * Reads the given InputStream as an excel format file (xls), and processes
     * the tabular data on the given sheet.  If no sheet is provided it reads the
     * first sheet of the workbook.
     *
     * @param name The name of the underlying file
     * @param stream The InputStream of the excel (xls) file
     * @param sheet The name of the sheet to process
     * @return The XlsSource of the underlying excel data.
     */
    public static XlsSource xls(String name, InputStream is, String sheet = null) {
        return new XlsSource( name, new POIFSFileSystem(is), sheet )
    }

    /**
     * Reads the given excelFile and pulls out all of the tabular data on the given sheet.
     * If not sheet is provided it will read the first sheet in the workbook.
     *
     * @param excelFile The excel file to read
     * @param sheet the name of the sheet to pull out, default is the first sheet.
     * @return The XlsSource of the underlying excel data.
     */
    public static XlsSource xls(File file, String sheet = null) {
        return new XlsSource( file.name, new POIFSFileSystem(file, true), sheet )
    }

    /**
     * Sets the row to start reading the table data.
     * @param row the starting row of the data.  Rows are 0-based indexes, and the first row is the header.  Default is 0.
     * @return the given XlsSource
     */
    XlsSource startOnRow( int row ) {
        this.startOnRow = row
        return this
    }

    /**
     * Sets the password if the xls file is protected.
     * @param password the give password to read the xls file.
     * @return The given XlsSource
     */
    XlsSource password(String password) {
        Biff8EncryptionKey.setCurrentUserPassword(password)
        return this
    }

    /**
     * Sets the values to output formula values vs the formula itself.  Default is true.
     * @param output
     * @return
     */
    XlsSource outputFormulaValues( boolean output ) {
        this.outputFormulaValues = output
        return this
    }

    @Override
    void doStart(Pipeline pipeline) {
        try {
            XlsProcessor processor = new XlsProcessor( pipeline, startOnRow, outputFormulaValues, sheet )
            processor.parse( pfs )
        } finally {
            Biff8EncryptionKey.setCurrentUserPassword(null)
        }
    }
}
