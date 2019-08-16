package gratum.operators

import gratum.etl.Pipeline

class PrintRowOperator implements Operator<Map,Map> {

    private String[] columns

    public static Operator<Map,Map> printRow(String... columns) {
        return new PrintRowOperator(columns)
    }

    PrintRowOperator(String... columns) {
        this.columns = columns
    }

    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {
        source.addStep("printRow(${columns})") { Map row ->
            if( columns ) {
                println( "[ ${columns.toList().collect { row[it] }.join(',')} ]" )
            } else {
                println( row )
            }
            return row
        }
        return source
    }
}
