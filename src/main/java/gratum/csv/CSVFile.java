package gratum.csv;

import org.apache.commons.io.input.BOMInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CSVFile implements Closeable {

    private File file;
    private Reader reader;
    private String separator;
    private PrintWriter writer;
    private String lastLine;

    private int rows = 0;
    private List<String> columnHeaders;
    private boolean escaped = true;
    private boolean writeBom = false;

    public CSVFile(String filename, String separator) {
        this( new File(filename), separator );
    }

    public CSVFile(File file, String separator) {
        this.file = file;
        this.separator = separator;
    }

    public CSVFile(Reader reader, String separator) {
        this.reader = reader;
        this.separator = separator;
    }

    public CSVFile(PrintWriter out, String separator) {
        this.writer = out;
        this.separator = separator;
    }

    public void setEscaped(boolean escaped) {
        this.escaped = escaped;
    }

    public int parse( CSVReader callback ) throws IOException {
        if( file != null ) {
            BOMInputStream bom = new BOMInputStream(new FileInputStream(file));
            Reader reader = bom.hasBOM() ? new InputStreamReader(bom, bom.getBOMCharsetName()) : new InputStreamReader(bom, StandardCharsets.UTF_8);
            return parse(reader, callback);
        } else {
            return parse(reader,callback);
        }
    }

    protected int parse(Reader reader, CSVReader callback) throws IOException {
        LineNumberReader lineNumberReader = new LineNumberReader(reader);
        int lines = 1;
        if( columnHeaders == null ) {
            try {
                columnHeaders = readNext(lineNumberReader);
                callback.processHeaders( columnHeaders );
                lines++;
            } catch( Exception ex ) {
                throw new IOException( getName() + ": Could not process header " + lines + ": " + lastLine, ex );
            }
        }

        try {
            List<String> row;
            while ((row = readNext(lineNumberReader)) != null) {
                boolean stop = callback.processRow(columnHeaders, row);
                if (stop) {
                    return lines;
                }
                lines++;
            }
            return lines;
        } catch( HaltPipelineException ex ) {
            throw ex;
        } catch( RuntimeException ex ) {
            throw new RuntimeException( getName() + ": Could not parse line " + lines + ": " +  lastLine, ex );
        } catch( Exception ex ) {
            throw new IOException( getName() + ": Could not process line " + lines + ": " + lastLine, ex );
        } finally {
            lineNumberReader.close();
            callback.afterProcessing();
        }
    }

    private String getName() {
        return file != null ? file.getName() : "<stream>";
    }

    private List<String> readNext( LineNumberReader reader ) throws IOException {
        do {
            lastLine = reader.readLine();
            if( lastLine == null ) return null;
        } while( lastLine.length() == 0 );

        return escaped ? parseColumnsWithEscaping() : parseColumnsWithoutEscaping();
    }

    private List<String> parseColumnsWithoutEscaping() {
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

    private List<String> parseColumnsWithEscaping() {
        List<String> line = new ArrayList<>( columnHeaders != null ? columnHeaders.size() : 10 );
        int columnStart = 0;
        char sep = separator.charAt(0);
        boolean skipSeparator = false;
        boolean stripQuotes = false;
        for( int i = 0; i < lastLine.length(); i++ ) {
            char currentChar = lastLine.charAt(i);
            if( currentChar == '"' ) {
                if( skipSeparator && isEscaped(i) ) {
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

    private boolean isEscaped(int i) {
        return i + 1 < lastLine.length() && lastLine.charAt(i+1) == '"';
    }

    private String unescape( String input ) {
        return input.replace("\\n", "\n").replace("\"\"", "\"");
    }

    public void write( Map row, String[] columnHeaders ) throws IOException {
        if( this.columnHeaders == null ) {
            this.columnHeaders = Arrays.asList( columnHeaders );
        }
        String[] rowArray = new String[this.columnHeaders.size()];
        int i = 0;
        for (String columnHeader : this.columnHeaders) {
            rowArray[i++] = row.get(columnHeader) == null ? "" : row.get(columnHeader).toString();
        }
        write((Object[]) rowArray);
    }

    public void write( Map row ) throws IOException {
        if (columnHeaders == null) {
            columnHeaders = new ArrayList<>( row.keySet().size() );
            int i = 0;
            for (Object headerKey : row.keySet()) {
                columnHeaders.add( headerKey.toString() );
            }
        }
        if( rows == 0 ) write(columnHeaders.toArray());
        String[] rowArray = new String[columnHeaders.size()];
        int i = 0;
        for (String columnHeader : columnHeaders) {
            if (row.get(columnHeader) == null) {
                rowArray[i++] = "";
            } else {
                rowArray[i++] = row.get(columnHeader).toString();
            }
        }
        write((Object[]) rowArray);
    }
    public void write( Object... row ) throws IOException {
        if( writer == null ) {
            if( !file.getParentFile().exists() ) file.getParentFile().mkdirs();
            // make sure we write BOM since excel seems to need this to recognize UTF8
            writer = new PrintWriter( new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8) );
            if( writeBom ) writer.print('\ufeff');
        }
        StringBuilder buffer = new StringBuilder();
        for( int i = 0; i < row.length; i++ ) {
            if( i > 0 ) {
                buffer.append(separator);
            }
            if( row[i] != null ) {
                buffer.append( escape( format( row[i] ) ) );
            }
        }
        writer.println( buffer.toString() );
        rows++;
    }

    private String format(Object o) {
        return o.toString();
    }

    private CharSequence escape(String source) {
        if( source.isEmpty() ) return source;
        StringBuilder builder = new StringBuilder( source.length() + 2 );
        builder.append('"');
        int lastIndex = 0;
        for( int i = 0; i < source.length(); i++ ) {
            char c = source.charAt(i);
            switch( c ) {
                case '"':
                    builder.append( source, lastIndex, i );
                    builder.append("\"\"");
                    lastIndex = i + 1;
                    break;
                case '\n':
                    builder.append( source, lastIndex, i );
                    builder.append("\\n");
                    lastIndex = i + 1;
                    break;
            }
        }
        if( lastIndex < source.length() ) {
            builder.append( source.subSequence(lastIndex, source.length() ) );
        }
        builder.append('"');
        return builder.toString();
    }

    public void flush() {
        if( writer != null ) {
            writer.flush();
        }
    }
    public void close() {
        flush();
        if( writer != null ) {
            writer.close();
        }
    }

    public int getRows() {
        return rows;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public List<String> getColumnHeaders() {
        return columnHeaders;
    }

    public void setColumnHeaders(List<String> columnHeaders) {
        this.columnHeaders = columnHeaders;
    }

    public void setWriteBom(boolean writeBom) {
        this.writeBom = writeBom;
    }
}
