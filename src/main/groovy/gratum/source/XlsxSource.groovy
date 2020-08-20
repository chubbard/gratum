package gratum.source

import gratum.etl.Pipeline
import org.apache.poi.openxml4j.opc.OPCPackage
import org.apache.poi.openxml4j.opc.PackageAccess
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.ss.usermodel.DateUtil
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.ss.usermodel.WorkbookFactory
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

class XlsxSource implements Source {

    String name
    File excelFile
    InputStream stream
    String sheet

    XlsxSource(String name, InputStream stream, String sheet = null) {
        this.name = name
        this.stream = stream
        this.sheet = sheet
    }


    XlsxSource(File excelFile, String sheet = null) {
        this.excelFile = excelFile
        this.name = excelFile.name
        this.sheet = sheet
    }

    public static XlsxSource xlsx(String name, InputStream stream, String sheet = null) {
        return new XlsxSource( name, stream, sheet )
    }

    public static XlsxSource xlsx( File file, String sheet = null ) {
        return new XlsxSource( file, sheet )
    }

    public Pipeline into() {
        Pipeline pipeline = new Pipeline( name )
        pipeline.src = this
        return pipeline
    }

    @Override
    void start(Pipeline pipeline) {
        OPCPackage ocp = null
        try {
            ocp = OPCPackage.open( stream ?: excelFile.newInputStream() )

            ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(ocp)
            XSSFReader xssfReader = new XSSFReader(ocp)
            StylesTable styles = xssfReader.getStylesTable()
            XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) xssfReader.getSheetsData()
            while( iter.hasNext() ) {
                InputStream is = iter.next()
                String name = iter.getSheetName()
                if( name == this.sheet || this.sheet == null) {
                    DataFormatter formatter = new DataFormatter();
                    InputSource sheetSource = new InputSource(is);
                    try {
                        XMLReader sheetParser = XMLHelper.newXMLReader();
                        XslxSheetHandler sheetHandler = new XslxSheetHandler( pipeline )
                        ContentHandler handler = new XSSFSheetXMLHandler(styles, null, strings, sheetHandler, formatter, false)
                        sheetParser.setContentHandler(handler)
                        sheetParser.parse(sheetSource);
                    } catch(ParserConfigurationException e) {
                        throw new RuntimeException("SAX parser configuration error: ${e.getMessage()}", e)
                    }
                    if( sheet == null ) sheet = name // prevent anymore sheets
                }
            }
        } finally {
            ocp?.close()
        }
/*
        Workbook wb = null
        if( excelFile ) wb = WorkbookFactory.create( excelFile )
        if( stream ) wb = WorkbookFactory.create( stream )

        try {
            Sheet s = sheet ? wb.getSheet(sheet) : wb.getSheetAt(0)
            Row header
            int line = 1
            for (Row r : s) {
                if (header == null) {
                    header = r
                } else {
                    Map<String, Object> row = [:]
                    for (Cell cell : r) {
                        int c = cell.getColumnIndex()
                        Cell headerCell = header.getCell(c)
                        Object value = null
                        switch (cell.cellType) {
                            case CellType.STRING:
                                value = cell.getStringCellValue()
                                break
                            case CellType.NUMERIC:
                                if (DateUtil.isCellDateFormatted(cell)) {
                                    value = cell.getDateCellValue()
                                } else {
                                    value = cell.getNumericCellValue()
                                }
                                break
                            case CellType.BOOLEAN:
                                value = cell.getBooleanCellValue()
                                break
                            case CellType.FORMULA:
                                // todo recursively evaluate the formula!
                                value = cell.getCellFormula()
                                break
                            case CellType.ERROR:
                                value = cell.getErrorCellValue()
                                break
                            case CellType.BLANK:
                            case CellType._NONE:
                                value = null
                                break
                        }
                        row[headerCell.getStringCellValue()] = value
                    }
                    pipeline.process(row, line)
                    line++
                }
            }
        } finally {
            wb.close()
        }
*/
    }

    class XslxSheetHandler implements XSSFSheetXMLHandler.SheetContentsHandler {

        int headerRow = -1;
        List<String> headers
        Pipeline pipeline
        Map current
        int col = 0
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
                current = [:]
            }
            currentRow = rowNum
            col = 0
        }

        @Override
        void endRow(int rowNum) {
            if( rowNum != headerRow ) {
                pipeline.process( current, rowNum)
            }
        }

        @Override
        void cell(String cellReference, String formattedValue, XSSFComment comment) {
            if( currentRow != headerRow ) {
                current[ headers[col] ] = formattedValue
            } else {
                headers.add( formattedValue )
            }
            col++
        }
    }

}
