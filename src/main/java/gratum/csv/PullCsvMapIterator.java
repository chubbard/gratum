package gratum.csv;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.*;

public class PullCsvMapIterator implements Iterator<Map<String, Object>> {

    CSVFile csv;
    LineNumberReader lineNumberReader;
    int lines = 1;
    Map<String,Object> nextRow;
    List<String> header;

    public PullCsvMapIterator(CSVFile aCsv, Reader reader) {
        csv = aCsv;
        this.lineNumberReader = new LineNumberReader(reader);
    }

    @Override
    public boolean hasNext() {
        try {
            if( nextRow == null ) {
                readNextRow();
            }
            return nextRow != null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void readNextRow() throws IOException {
        List<String> row = csv.readNext(lineNumberReader);
        if( header == null && row != null ) {
            header = row;
            row = csv.readNext(lineNumberReader);
        }
        if( row != null ) {
            nextRow = new HashMap<>();
            for( int i = 0; i < header.size(); i++ ) {
                nextRow.put(header.get(i), row.get(i));
            }
            lines++;
        }
    }

    @Override
    public Map<String, Object> next() {
        try {
            if (nextRow == null) {
                readNextRow();
            }
            Map<String, Object> r = nextRow;
            nextRow = null;
            return r;
        } catch( IOException ex ) {
            throw new RuntimeException(ex);
        }
    }
}
