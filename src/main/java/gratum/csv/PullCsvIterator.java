package gratum.csv;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;

public class PullCsvIterator implements Iterator<List<String>> {
    CSVFile csv;
    int lines = 1;
    List<String> nextRow;
    LineNumberReader lineNumberReader;

    public PullCsvIterator(CSVFile aCsv, Reader reader) {
        csv = aCsv;
        lineNumberReader = new LineNumberReader(reader);
    }

    @Override
    public boolean hasNext() {
        try {
            nextRow = csv.readNext(lineNumberReader);
            lines++;
            return nextRow != null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> next() {
        try {
            if( nextRow == null ) {
                nextRow = csv.readNext(lineNumberReader);
                lines++;
            }
            List<String> r = nextRow;
            nextRow = null;
            return r;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
