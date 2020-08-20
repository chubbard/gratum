package gratum.source

import gratum.csv.CSVFile
import gratum.csv.CSVReader
import gratum.etl.Pipeline


/**
 * Reads a delimited separated text file into a series of rows.  It's most common format is the comma separated values (csv),
 * but this supports any value separated format (i.e. tab, colon, comma, pipe, etc).  Here is a simple example:
 *
 * <pre>
 *     csv( "/resources/titanic.csv" ).filter([ Embarked: "Q"]).go()
 * </pre>
 *
 * Example changing the delimiter:
 *
 * <pre>
 *     csv("/resources/pipe_separated_example.csv", "|")
 *          .filter([ someProperty: "someValue" ])
 *          .go()
 * </pre>
 *
 * Example header-less file:
 *
 * <pre>
 *     csv("/resources/headerless.csv", "|", ["date", "status", "client-ip", "server-name", "url", "length", "thread", "user-agent", "referer"])
 *          .filter { Map row -> row["server-name}.contains("myhostname") }
 *          .go()
 * </pre>
 *
 * From external InputStream
 *
 * <pre>
 *     csv( "External InputStream", stream, "|" ).filter( [ someColumn: "someValue" ] ).go()
 * </pre>
 */
public class CsvSource extends AbstractSource {
    CSVFile csvFile

    CsvSource(File file, String separator = ",", List<String> headers = null) {
        this.name = file.name
        csvFile = new CSVFile( file, separator );
        if( headers ) csvFile.setColumnHeaders( headers )
    }

    CsvSource( Reader reader, String separator = ",", List<String> headers = null) {
        this.name = "Reader"
        csvFile = new CSVFile( reader, separator )
        if( headers ) csvFile.setColumnHeaders( headers )
    }

    public static CsvSource csv( String filename, String separator = ",", List<String> headers = null ) {
        return new CsvSource( new File(filename), separator, headers )
    }

    public static CsvSource csv(String name, InputStream stream, String separator = ",", List<String> headers = null) {
        return new CsvSource( new InputStreamReader(stream), separator, headers )
    }

    @Override
    void start(Pipeline pipeline) {
        int line = 1
        CSVReader csvReader = new CSVReader() {
            @Override
            void processHeaders(List<String> header) {
            }

            @Override
            boolean processRow(List<String> header, List<String> row) {
                Map obj = [:]
                for( int i = 0; i < header.size(); i++ ) {
                    obj[header[i]] = i < row.size() ? row[i] : null
                }

                return pipeline.process( obj, line++ )
            }

            @Override
            void afterProcessing() {

            }
        }

        csvFile.parse(csvReader)
    }
}
