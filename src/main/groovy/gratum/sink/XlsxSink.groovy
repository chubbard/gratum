package gratum.sink

import gratum.etl.FileOpenable
import gratum.etl.Pipeline
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.RichTextString
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.xssf.streaming.SXSSFWorkbook

import java.time.LocalDate
import java.time.LocalDateTime

class XlsxSink implements Sink<Map<String,Object>> {

    File output
    SXSSFWorkbook workbook
    Sheet sheet
    Collection<String> columns = null

    XlsxSink(File output, String sheetName = "Sheet 1") {
        this.output = output
        this.workbook = new SXSSFWorkbook(100)
        this.sheet = workbook.createSheet( sheetName )
    }

    @Override
    String getName() {
        return output.name
    }

    @Override
    void attach(Pipeline pipeline) {
        int r = 0
        pipeline.addStep("xlsxOut(${this.name}}") { Map<String,Object> row ->
            if( !columns ) {
                columns = row.keySet()
            }
            Row xlsxRow = sheet.createRow( r++ )
            columns.eachWithIndex { String column, int columnIndex ->
                Cell cell = xlsxRow.createCell( columnIndex )
                Object val = row[column]
                if( val instanceof CharSequence ) {
                    cell.setCellValue( val.toString() )
                } else if( val instanceof Number ) {
                    cell.setCellValue( ((Number)val).doubleValue() )
                } else if( val instanceof Boolean ) {
                    cell.setCellValue( val as Boolean )
                } else if( val instanceof Date ) {
                    cell.setCellValue( val as Date )
                } else if( val instanceof LocalDate ) {
                    cell.setCellValue( val as LocalDate )
                } else if( val instanceof LocalDateTime ) {
                    cell.setCellValue( val as LocalDateTime )
                } else if( val instanceof Calendar ) {
                    cell.setCellValue( val as Calendar )
                } else if( val instanceof RichTextString ) {
                    cell.setCellValue( val as RichTextString )
                } else {
                    cell.setCellValue( val.toString() )
                }
            }
            row
        }
    }

    @Override
    Map<String, Object> getResult() {
        return [ file: output.name, filename: this.name, stream: new FileOpenable(output) ]
    }

    @Override
    void close() throws IOException {
        output.withOutputStream {stream ->
            workbook.write(stream )
        }
        workbook.dispose()
    }
}
