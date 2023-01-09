package gratum.csv;

import org.apache.commons.io.input.BOMInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CSVPullReader implements Closeable  {

    private boolean headerFirstLine = true;
    private boolean escaped = true;

    private String separator;

    private int lines = 0;
    private LineNumberReader reader;

    private List<String> columnHeaders;

    private String lastLine;

    public CSVPullReader( File file, String separator ) throws IOException {
        this( new FileInputStream(file), separator );
    }

    public CSVPullReader( InputStream stream, String separator ) throws IOException {
        BOMInputStream bom = new BOMInputStream(stream);
        this.reader = new LineNumberReader(bom.hasBOM() ? new InputStreamReader(bom, bom.getBOMCharsetName()) : new InputStreamReader(bom, StandardCharsets.UTF_8));
        this.separator = separator;
    }

    public CSVPullReader( Reader reader, String separator ) {
        this.reader = new LineNumberReader( reader );
        this.separator = separator;
    }

    public List<String> nextRow() throws IOException {
        if( lines == 0 && headerFirstLine ) {
            columnHeaders = readNext();
        }
        return readNext();
    }

    @Override
    public void close() throws IOException {
        lastLine = null;
        columnHeaders = null;
        reader.close();
    }

    private List<String> readNext() throws IOException {
        do {
            lastLine = reader.readLine();
            if( lastLine == null ) return null;
            lines++;
        } while( lastLine.length() == 0 );

        return escaped ? parseColumnsWithEscaping(lastLine) : parseColumnsWithoutEscaping(lastLine);
    }

    private List<String> parseColumnsWithoutEscaping(String lastLine) {
        List<String> row = new ArrayList<>( columnHeaders != null ? columnHeaders.size() : 10 );
        int columnStart = 0;
        while( columnStart < lastLine.length() ) {
            int index = lastLine.indexOf( separator, columnStart );
            if( index < 0 ) {
                row.add( columnStart == 0 ? lastLine : lastLine.substring( columnStart ) );
                columnStart = lastLine.length();
            } else {
                row.add( lastLine.substring(columnStart, index ) );
                columnStart = index + separator.length();
            }
        }
        if( columnStart == lastLine.length() && lastLine.endsWith(separator) ) {
            // we found a trailing separator issue which means we didn't get a separator
            row.add("");
        }
        return row;
    }

    private List<String> parseColumnsWithEscaping(String lastLine) {
        List<String> line = new ArrayList<>( columnHeaders != null ? columnHeaders.size() : 10 );
        int columnStart = 0;
        char sep = separator.charAt(0);
        boolean skipSeparator = false;
        boolean stripQuotes = false;
        for( int i = 0; i < lastLine.length(); i++ ) {
            char currentChar = lastLine.charAt(i);
            if( currentChar == '"' ) {
                if( skipSeparator && isEscaped(lastLine, i) ) {
                    i++;
                } else {
                    skipSeparator = !skipSeparator;
                    stripQuotes = stripQuotes || i == columnStart;
                }
            } else if( !skipSeparator && sep == currentChar ) {
                String content = stripQuotes ? lastLine.substring( columnStart + 1, i - 1 ) : lastLine.substring( columnStart, i );
                line.add( unescape(content) );
                columnStart = i + 1;
                stripQuotes = false;
            }
        }

        if( columnStart < lastLine.length() ) {
            String content = stripQuotes ? lastLine.substring( columnStart + 1, lastLine.length() - 1 ) : lastLine.substring( columnStart );
            line.add( unescape(content) );
        } else {
            // we have a trailing comma at the end without anything after it so add an empty string.
            line.add( "" );
        }

        return line;
    }

    private boolean isEscaped(String lastLine, int i) {
        return i + 1 < lastLine.length() && lastLine.charAt(i+1) == '"';
    }

    private String unescape( String input ) {
        return input.replace("\\n", "\n").replace("\"\"", "\"");
    }

    public int getLines() {
        return lines;
    }

    public List<String> getColumnHeaders() {
        return columnHeaders;
    }

    public String getLastLine() {
        return lastLine;
    }

    public CSVPullReader escaped(boolean escaped) {
        this.escaped = escaped;
        return this;
    }
}
