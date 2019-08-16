package gratum.operators

import gratum.etl.Pipeline
import gratum.etl.RejectionCategory

import java.text.ParseException
import java.text.SimpleDateFormat

class DateOperator implements Operator<Map,Map> {

    String column
    String formatSpec
    SimpleDateFormat dateFormat

    /**
     * Parses the string at the given column name into a Date object using the given format.  Any value not
     * parseable by the format is rejected.
     * @param column The field to use to find the string value to parse
     * @param format The format of the string to use to parse into a java.util.Date
     * @return An Operator where all rows contain a java.util.Date at given field name
     */
    public static Operator<Map,Map> asDate(String column, String format = "yyyy-MM-dd") {
        return new DateOperator( column, format )
    }

    DateOperator(String column, String format) {
        this.column = column
        this.formatSpec = format
        this.dateFormat = new SimpleDateFormat(this.formatSpec)
    }

    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {
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
