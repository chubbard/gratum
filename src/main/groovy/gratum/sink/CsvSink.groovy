package gratum.sink

import gratum.csv.CSVFile
import gratum.etl.FileOpenable
import gratum.etl.Pipeline

class CsvSink implements Sink<Map<String,Object>> {

    CSVFile csvFile

    public CsvSink(String filename, String separator, List<String> headers = null) {
        this.csvFile = new CSVFile( filename, separator )
        if( headers ) {
            this.csvFile.setColumnHeaders( headers )
        }
    }

    @Override
    String getName() {
        return csvFile.getFile().name
    }

    @Override
    void attach(Pipeline pipeline) {
        pipeline.addStep("csvOut(${csvFile.file.name})") { Map row ->
            csvFile.write( row )
            row
        }
    }

    @Override
    Map<String, Object> getResult() {
        return [ file: csvFile.file, filename: this.name, stream: new FileOpenable(csvFile.file) ]
    }

    @Override
    void close() throws IOException {
        csvFile.close()
    }
}
