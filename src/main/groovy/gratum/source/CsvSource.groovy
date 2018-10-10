package gratum.source

import gratum.csv.CSVFile
import gratum.csv.CSVReader
import gratum.etl.Pipeline


/**
 * Created by charliehubbard on 7/11/18.
 */
public class CsvSource implements Source {
    CSVFile csvFile

    CsvSource(File file, String separator = ",") {
        csvFile = new CSVFile( file, separator );
    }

    CsvSource( Reader reader, String separator = ",") {
        csvFile = new CSVFile( reader, separator )
    }

    public static Pipeline csv( String filename, String separator = "," ) {
        Pipeline pipeline = new Pipeline( filename )
        pipeline.src = new CsvSource( new File(filename), separator )
        return pipeline
    }

    public static Pipeline csv(String name, InputStream stream, String separator = ",") {
        Pipeline pipeline = new Pipeline(name)
        pipeline.src = new CsvSource( new InputStreamReader(stream), separator )
        return pipeline
    }

    @Override
    void start(Closure callback) {
        CSVReader csvReader = new CSVReader() {
            @Override
            void processHeaders(List<String> header) {
            }

            @Override
            boolean processRow(List<String> header, List<String> row) {
                Map obj = [:]
                for( int i = 0; i < row.size(); i++ ) {
                    obj[header[i]] = row[i]
                }

                return callback( obj )
            }

            @Override
            void afterProcessing() {

            }
        }

        csvFile.parse(csvReader)
    }
}
