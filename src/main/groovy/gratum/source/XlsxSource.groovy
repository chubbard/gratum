package gratum.source

import gratum.etl.LoadStatistic
import gratum.etl.Pipeline
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.DateUtil
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.ss.usermodel.WorkbookFactory

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

    public LoadStatistic attach( Closure<Pipeline> setup ) {
        Pipeline pipeline = new Pipeline( name )
        pipeline.src = this
        Pipeline r = setup( pipeline )
        r.go()
    }

    @Override
    void start(Pipeline pipeline) {
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
    }
}
