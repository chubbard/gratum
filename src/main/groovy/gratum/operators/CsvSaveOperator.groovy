package gratum.operators

import gratum.csv.CSVFile
import gratum.etl.Pipeline
import gratum.etl.PipelineOutput
import gratum.source.SingleSource

class CsvSaveOperator implements Operator<Map,PipelineOutput<Map>> {

    private String[] columns
    private CSVFile out

    static Operator<Map,PipelineOutput<Map>> saveAsCsv(String filename, String separator ) {
        CSVFile out = new CSVFile( filename, separator )
        return new CsvSaveOperator( out )
    }

    static Operator<Map,PipelineOutput<Map>> saveAsCsv(String filename, String separator, String... columns) {
        CSVFile out = new CSVFile( filename, separator )
        return new CsvSaveOperator( out, columns )
    }

    CsvSaveOperator(CSVFile out, String[] columns = null) {
        this.out = out
        this.columns = columns
    }

    @Override
    Pipeline<PipelineOutput<Map>> attach(Pipeline<Map> source) {
        boolean first = true
        PipelineOutput<Map> out = new PipelineOutput<>({ Map row, OutputStream os ->

            if( first ) {
                out.setWriter( new OutputStreamWriter( os ) )
                first = false
            }

            if( columns ) {
                out.write( row, columns )
            } else {
                out.write( row )
            }
        })

        Pipeline<PipelineOutput<Map>> pipe = new Pipeline<>( source.name )
        pipe.src = new SingleSource( out )

        out.attach( source )

        source.after {
            this.out.close()
        }

        return pipe
    }
}
