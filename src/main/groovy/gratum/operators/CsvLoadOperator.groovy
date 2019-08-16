package gratum.operators

import gratum.etl.Pipeline
import gratum.source.CsvSource
import gratum.source.SingleSource

class CsvLoadOperator implements Operator<Reader,Map<String,Object>> {

    private String separator

    public static Pipeline<Map<String,Object>> csv( String filename, String separator = ",") {
        File file = new File( filename )
        return SingleSource.of( (Reader)file.newReader(), filename )
                .add( new CsvLoadOperator(separator) )
    }

    CsvLoadOperator(String separator) {
        this.separator = separator
    }

    @Override
    Pipeline<Map<String,Object>> attach(Pipeline<Reader> source) {
        return source.exchange(source.name) { Reader reader ->
            Pipeline<Map<String,Object>> pipeline = new Pipeline<>( source.name )
            pipeline.src = new CsvSource( reader, separator )
            pipeline.after {
                reader.close()
            }
            return pipeline
        }
    }
}
