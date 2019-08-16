package gratum.operators

import gratum.etl.Pipeline
import gratum.etl.RejectionCategory

import java.text.ParseException
import java.text.SimpleDateFormat

class DateOperator implements Operator<Map<String,Object>,Map<String,Object>> {

    String column
    String formatSpec
    SimpleDateFormat dateFormat

    DateOperator(String column, String format) {
        this.column = column
        this.formatSpec = format
        this.dateFormat = new SimpleDateFormat(this.formatSpec)
    }

    @Override
    Pipeline<Map<String,Object>> attach(Pipeline<Map<String,Object>> source) {
        source.addStep("asDate(${column}, ${formatSpec})") { Map row ->
            String val = row[column] as String
            try {
                if (val) row[column] = dateFormat.parse(val)
                return row
            } catch( ParseException ex ) {
                source.reject( "${row[column]} could not be parsed by format ${formatSpec}", RejectionCategory.INVALID_FORMAT )
                return null
            }
        }
        return source

    }
}
