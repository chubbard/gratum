package gratum.csv;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ExternalSort {

    final int bufferLimit;
    final int mergeFactor;

    List<Map<String,Object>> buffer;

    List<CSVFile> files = new ArrayList<>();

    Comparator<Map<String,Object>> comparator;

    File tmpDir;

    public ExternalSort(int bufferSize, int mergeAfter) throws IOException {
        mergeFactor = mergeAfter;
        bufferLimit = bufferSize;

        buffer = new ArrayList<>(bufferSize);
        tmpDir = File.createTempFile("external_sort", "");
        tmpDir.mkdirs();
    }

    public ExternalSort sortBy(String... columns) {
        comparator = (o1, o2) -> {
            for( String col : columns ) {
                int comp = ((Comparable)o1.get( col )).compareTo( o2.get(col) );
                if( comp != 0 ) {
                    return comp;
                }
            }
            return 0;
        };
        return this;
    }

    public void add( Map<String,Object> row ) throws IOException {
        buffer.add( row );
        if( buffer.size() == bufferLimit ) {
            buffer.sort( comparator );
            saveBuffer();
        }
    }

    private void saveBuffer() throws IOException {
        try ( CSVFile csvFile = new CSVFile( new File( tmpDir, "sort-" + files.size() + ".csv" ), "," ) ) {
            for( Map<String,Object> row : buffer ) {
                csvFile.write( row );
            }
            buffer.clear();
            files.add( csvFile );
        }

        if( files.size() == mergeFactor ) {
            mergeFiles();
        }
    }

    private void mergeFiles() {
        // todo read csv file(s) and merge the files together, CSVFile needs to support pull operation
    }
}
