package gratum.source

import gratum.csv.CSVFile
import gratum.csv.CSVReader
import gratum.etl.Pipeline
import groovy.transform.CompileStatic


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
 *
 * Using {@link #of} to configure CsvSource.
 *
 * <pre>
 *     CsvSource.of("/resources/titanic-unescaped.csv", "," ).escaping(false).into()
 *          .filter([ Embarked: "Q"])
 *          .go()
 * </pre>
 */
@CompileStatic
public class CsvSource extends AbstractSource {

    CSVFile csvFile

    Closure<Void> headerClosure = null

    CsvSource(File file, String separator = ",", List<String> headers = null) {
        super(file.name)
        csvFile = new CSVFile( file, separator );
        if( headers ) csvFile.setColumnHeaders( headers )
    }

    CsvSource( Reader reader, String separator = ",", List<String> headers = null) {
        super("Reader")
        csvFile = new CSVFile( reader, separator )
        if( headers ) csvFile.setColumnHeaders( headers )
    }

    public static CsvSource of(File file, String separator = ",", List<String> headers = null) {
        return new CsvSource( file, separator, headers )
    }

    public static CsvSource of(String filename, String separator = ",", List<String> headers = null) {
        return of( new File( filename ), separator, headers )
    }

    public static CsvSource of(String filename, InputStream stream, String separator = ",", List<String> headers = null) {
        return (CsvSource)new CsvSource( new InputStreamReader(stream), separator, headers ).name(filename)
    }

    public static Pipeline csv( File filename, String separator = ",", List<String> headers = null ) {
        return new CsvSource( filename, separator, headers ).into()
    }

    public static Pipeline csv( String filename, String separator = ",", List<String> headers = null ) {
        return new CsvSource( new File(filename), separator, headers ).into()
    }

    public static Pipeline csv(String name, InputStream stream, String separator = ",", List<String> headers = null) {
        return new CsvSource( new InputStreamReader(stream), separator, headers ).name(name).into()
    }

    /**
     * Register a closure to receive the header before processing starts
     * @param headerClosure closure that is passed the List<String> header as a parameter
     * @return this
     */
    public CsvSource header( Closure<Void> headerClosure ) {
        this.headerClosure = headerClosure
        return this
    }

    public void setSeparator(String separator ) {
        csvFile.setSeparator( separator )
    }

    public void setHeaders(List<String> headers) {
        csvFile.setColumnHeaders( headers )
    }

    public void setEscaped(boolean escaped) {
        csvFile.setEscaped(escaped)
    }

    /**
     * Turn on/off CSV escaping rules.  Some files do not properly escape columns so turning
     * off those rules can help in parsing such files.  This means columns surrounded in double
     * quotes will not be removed, or handling embedded separators within columns.  Turning off
     * escaping renders the parsing as a straight separator parse.  The default is escaping is
     * turned on.
     *
     * @param escaped turn on (ie true) or turn off (ie false) escaping
     * @return this
     */
    public CsvSource escaping( boolean escaped ) {
        csvFile.setEscaped( escaped )
        return this
    }

    @Override
    void doStart(Pipeline pipeline) {
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
