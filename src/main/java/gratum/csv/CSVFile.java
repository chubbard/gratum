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
        try( CSVPullReader csvPullReader = new CSVPullReader( reader, separator ) ) {
            try {
                List<String> row;
                while ((row = csvPullReader.nextRow()) != null) {
                    if( columnHeaders == null ) {
                        columnHeaders = csvPullReader.getColumnHeaders();
                        callback.processHeaders(columnHeaders);
                    }
                    boolean stop = callback.processRow(columnHeaders, row);
                    if (stop) {
                        return csvPullReader.getLines();
                    }
                }
                return csvPullReader.getLines();
            } catch (HaltPipelineException ex) {
                throw ex;
            } catch (RuntimeException ex) {
                throw new RuntimeException(getName() + ": Could not parse line " + csvPullReader.getLines() + ": " + csvPullReader.getLines(), ex);
            } catch (Exception ex) {
                throw new IOException(getName() + ": Could not process line " + csvPullReader.getLines() + ": " + csvPullReader.getLastLine(), ex);
            } finally {
                callback.afterProcessing();
            }
        }
    }

    private String getName() {
        return file != null ? file.getName() : "<stream>";
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

    public CSVPullReader open() throws IOException {
        //noinspection resource
        return new CSVPullReader( file, separator ).escaped( escaped );
    }
}
