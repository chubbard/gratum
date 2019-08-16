package gratum.operators

import gratum.etl.Pipeline
import gratum.source.CsvSource
import gratum.source.SingleSource

class CsvLoadOperator implements Operator<Reader,Map<String,Object>> {

    private String separator

    public static Pipeline<Map<String,Object>> csv( String filename, String separator = ",") {
        return SingleSource.of( (Reader)new File( filename ).newReader(), filename )
                .add( new CsvLoadOperator() )
    }

    CsvLoadOperator(String separator) {
        this.separator = separator
    }

    @Override
    Pipeline<Map<String,Object>> attach(Pipeline<Reader> source) {
        Pipeline<Map<String,Object>> next = new Pipeline<>( source.name )

        source.addStep("loadCsv") { Reader reader ->
            next.src = new CsvSource( reader, separator )
            next.after {
                reader.close()
            }
            return reader
        }

        return next
    }
}
