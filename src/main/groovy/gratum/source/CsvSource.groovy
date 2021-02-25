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

    Closure<Void> headerClosure = null

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

    public static CsvSource of(File file, String separator = ",", List<String> headers = null) {
        return new CsvSource( file, separator, headers )
    }

    public static CsvSource of(String filename, String separator = ",", List<String> headers = null) {
        return of( new File( filename ), separator )
    }

    public static Pipeline csv( File filename, String separator = ",", List<String> headers = null ) {
        return new CsvSource( filename, separator, headers ).into()
    }

    public static Pipeline csv( String filename, String separator = ",", List<String> headers = null ) {
        return new CsvSource( new File(filename), separator, headers ).into()
    }

    public static Pipeline csv(String name, InputStream stream, String separator = ",", List<String> headers = null) {
        return new CsvSource( new InputStreamReader(stream), separator, headers ).into()
    }

    public CsvSource header( Closure<Void> headerClosure ) {
        this.headerClosure = headerClosure
        return this
    }

    @Override
    void start(Pipeline pipeline) {
        int line = 1
        CSVReader csvReader = new CSVReader() {
            @Override
            void processHeaders(List<String> header) {
                if( headerClosure ) {
                    headerClosure.call( header )
                }
            }

            @Override
            boolean processRow(List<String> header, List<String> row) {
                Map obj = [:]
                for( int i = 0; i < row.size(); i++ ) {
                    obj[header[i]] = row[i]
                }

                if( header.size() > row.size() ) {
                    for( int j = row.size(); j < header.size(); j++ ) {
                        obj[header[j]] = null
                    }
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
