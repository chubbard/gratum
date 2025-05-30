package gratum.source

import gratum.etl.Pipeline
import groovy.transform.CompileStatic
import org.apache.poi.openxml4j.opc.OPCPackage
import org.apache.poi.poifs.crypt.Decryptor
import org.apache.poi.poifs.crypt.EncryptionInfo
import org.apache.poi.poifs.crypt.EncryptionMode
import org.apache.poi.poifs.filesystem.POIFSFileSystem
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.ss.util.CellAddress
import org.apache.poi.util.LittleEndianInputStream
import org.apache.poi.util.XMLHelper
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable
import org.apache.poi.xssf.eventusermodel.XSSFReader
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler
import org.apache.poi.xssf.model.StylesTable
import org.apache.poi.xssf.usermodel.XSSFComment
import org.xml.sax.InputSource
import org.xml.sax.XMLReader
import org.xml.sax.ContentHandler

import javax.xml.parsers.ParserConfigurationException
import java.security.KeyException

/**
 * A {@link gratum.source.Source} that implements reading excel workbooks in xlsx
 * format.
 */
@CompileStatic
class XlsxSource extends AbstractSource {

    File excelFile
    InputStream stream
    String sheet
    Closure<Void> headerClosure = null
    String dateFormat = "yyyy-MM-dd"
    String password

    /**
     * Reads the given InputStream as an excel format file (xlsx), and processes
     * the tabular data on the given sheet.  If no sheet is provided it reads the
     * first sheet of the workbook.
     *
     * @param name The name of the underlying file
     * @param stream The InputStream of the excel (xlsx) file
     * @param sheet The name of the sheet to process
     */
    XlsxSource(String name, InputStream stream, String sheet = null) {
        super(name)
        this.stream = stream
        this.sheet = sheet
    }

    /**
     * Reads the given excelFile and pulls out all of the tabular data on the given sheet.
     * If not sheet is provided it will read the first sheet in the workbook.
     *
     * @param excelFile The excel file to read
     * @param sheet the name of the sheet to pull out, default is the first sheet.
     */
    XlsxSource(File excelFile, String sheet = null) {
        super(excelFile.name)
        this.excelFile = excelFile
        this.sheet = sheet
    }

    /**
     * Reads the given InputStream as an excel format file (xlsx), and processes
     * the tabular data on the given sheet.  If no sheet is provided it reads the
     * first sheet of the workbook.
     *
     * @param name The name of the underlying file
     * @param stream The InputStream of the excel (xlsx) file
     * @param sheet The name of the sheet to process
     * @return The XlsxSource of the underlying excel data.
     */
    public static XlsxSource xlsx(String name, InputStream stream, String sheet = null) {
        return new XlsxSource( name, stream, sheet )
    }

    /**
     * Reads the given excelFile and pulls out all of the tabular data on the given sheet.
     * If not sheet is provided it will read the first sheet in the workbook.
     *
     * @param excelFile The excel file to read
     * @param sheet the name of the sheet to pull out, default is the first sheet.
     * @return The XlsxSource of the underlying excel data.
     */
    public static XlsxSource xlsx( File file, String sheet = null ) {
        return new XlsxSource( file, sheet )
    }

    /**
     * Sets the default format for dates (default format is yyyy-MM-dd).  POI tends to return dates in Locale formats
     * which aren't always convenient for working with so this sets the date format using
     * the SimpleDateFormat syntax in Java.
     * @param format format string following the rules set out in SimpleDateFormat
     * @return this instance
     */
    public XlsxSource dateFormat(String format) {
        this.dateFormat = format
        return this
    }

    /**
     * The password to use for encrypted spreadsheets
     * @param password the given password to use to decrypt the stylesheet.
     * @return this instance
     */
    public XlsxSource password(String password) {
        this.password = password
        return this
    }

    void setSheet(String sheet) {
        this.sheet = sheet
    }

    void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat
    }

    void setPassword(String password) {
        this.password = password
    }

    @Override
    void doStart(Pipeline pipeline) {
        OPCPackage ocp = null
        try {
            InputStream istream = null
            if( password ) {
                if( excelFile ) {
                    POIFSFileSystem filesystem  = new POIFSFileSystem(excelFile)
                    EncryptionInfo info = new EncryptionInfo(filesystem)
                    Decryptor decryptor = Decryptor.getInstance(info)
                    if( decryptor.verifyPassword(password) ) {
                        istream = decryptor.getDataStream( filesystem )
                    } else {
                        throw new KeyException("Password is missing or incorrect.")
                    }
                } else if( stream ) {
                    // todo having trouble with this block
                    EncryptionInfo info = new EncryptionInfo(new LittleEndianInputStream(stream), EncryptionMode.standard)
                    Decryptor decryptor = Decryptor.getInstance(info)
                    if( decryptor.verifyPassword(password) ) {
                        istream = decryptor.getDataStream(stream, 0, 0)
                    } else {
                        throw new KeyException("Password is missing or incorrect.")
                    }
                } else {
                    throw new RuntimeException("Must specify a file or InputStream to load.")
                }
            } else {
                istream = stream ?: excelFile.newInputStream()
            }

            ocp = OPCPackage.open( istream )

            ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(ocp)
            XSSFReader xssfReader = new XSSFReader(ocp)
            StylesTable styles = xssfReader.getStylesTable()
            XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) xssfReader.getSheetsData()
            while( iter.hasNext() ) {
                InputStream is = iter.next()
                String name = iter.getSheetName()
                if( name == this.sheet || this.sheet == null) {
                    DataFormatter formatter = new CustomDateDataFormatter(dateFormat)
                    InputSource sheetSource = new InputSource(is)
                    try {
                        XMLReader sheetParser = XMLHelper.newXMLReader()
                        XslxSheetHandler sheetHandler = new XslxSheetHandler( pipeline )
                        ContentHandler handler = new XSSFSheetXMLHandler(styles, null, strings, sheetHandler, formatter, false)
                        sheetParser.setContentHandler(handler)
                        sheetParser.parse(sheetSource)
                    } catch(ParserConfigurationException e) {
                        throw new RuntimeException("SAX parser configuration error: ${e.getMessage()}", e)
                    }
                    if( sheet == null ) sheet = name // prevent anymore sheets
                }
            }
        } finally {
            ocp?.close()
        }
    }

    /**
     * Attaches a closure that will be called back when the headers are available.
     *
     * @param headerClosure the closure to call passing the headers as a {@link java.util.List}
     * @return The XlsxSource for method chaining
     */
    public XlsxSource header( Closure<Void> headerClosure ) {
        this.headerClosure = headerClosure
        return this
    }

    class XslxSheetHandler implements XSSFSheetXMLHandler.SheetContentsHandler {

        int headerRow = -1
        List<String> headers
        Pipeline pipeline
        Map current
        int currentRow = 0

        XslxSheetHandler(Pipeline pipeline) {
            this.pipeline = pipeline
        }

        @Override
        void startRow(int rowNum) {
            if( headers == null ) {
                headers = []
                headerRow = rowNum
            } else {
                // make sure we start with all headers included because if the first row doesn't have any values
                // in the last columns won't make it into the row.  It will produce ragged results
                current = headers.collectEntries() { e -> [e,null] }
            }
            currentRow = rowNum
        }

        @Override
        void endRow(int rowNum) {
            if( rowNum != headerRow ) {
                if( !current.isEmpty() ) {
                    pipeline.process( current, rowNum)
                }
            } else if( headerClosure ) {
                headerClosure.call( headers )
            }
        }

        @Override
        void cell(String cellReference, String formattedValue, XSSFComment comment) {
            if( currentRow != headerRow ) {
                CellAddress cellRef = new CellAddress( cellReference)
                current[ headers[cellRef.getColumn()] ] = formattedValue
                if( current.size() - 1 < cellRef.getColumn() ) {
                    // we encountered a skip, add missing cols
                    for( int i = current.size() - 1; i < cellRef.getColumn(); i++ ) {
                        current[ headers[i] ] = null
                    }
                }
            } else {
                headers.add( formattedValue )
            }
        }
    }

}
