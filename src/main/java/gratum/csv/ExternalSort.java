package gratum.csv;

import groovy.lang.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ExternalSort {

    private int bufferLimit;

    private List<Map<String,Object>> buffer;

    private List<String> sortByColumns;

    private Comparator<Map<String,Object>> comparator;

    private Comparator<List<String>> rowComparator;

    private final File tmpDir;

    private ExecutorService workers;

    private ArrayBlockingQueue<Tuple2<File,File>> mergeQueue = new ArrayBlockingQueue<>(100);

    public ExternalSort() throws IOException {
        tmpDir = File.createTempFile("external_sort", "");
        tmpDir.mkdirs();
        withBuffer( 50000 );
    }

    public ExternalSort withBuffer( int bufferSize ) {
        bufferLimit = bufferSize;
        buffer = new ArrayList<>(bufferSize);
        return this;
    }

    public ExternalSort sortBy(String... columns) {
        sortByColumns = Arrays.asList(columns);
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
        try ( CSVFile csvFile = new CSVFile( getFilenameFromBuffer(), "," ) ) {
            for( Map<String,Object> row : buffer ) {
                csvFile.write( row );
            }
            buffer.clear();
        }
    }

    private File getFilenameFromBuffer() {
        String filename = buffer.stream().findFirst()
                .map((x) -> sortByColumns.stream()
                        .map(c -> x.get(c).toString() )
                        .collect( Collectors.joining("_" ) ) )
                .get();
        File bufferFile = new File( tmpDir, filename + ".csv" );
        int i = 1;
        while( bufferFile.exists() ) {
            bufferFile = new File( tmpDir, filename + "_" + i + ".csv" );
            i++;
        }
        return bufferFile;
    }

    public File mergeFiles() throws IOException {
        File[] listing = tmpDir.listFiles();
        List<File> files = listing != null ? Arrays.asList(listing) : Collections.emptyList();
        try {
            do {
                files = files.stream().sorted(Comparator.comparing(File::getName)).collect(Collectors.toList());

                List<CompletableFuture<File>> mergedFiles = new ArrayList<>();
                for (int i = 0; i < files.size(); i += 2) {
                    if (i + 1 < files.size()) {
                        final File a = files.get(i);
                        final File b = files.get(i+1);
                        mergedFiles.add( CompletableFuture.supplyAsync(() -> {
                            try {
                                return mergeFiles(a, b);
                            } catch(IOException ioe) {
                                throw new CompletionException("Could not merge files " + a.getName() + " and " + b.getName() + " due to exception", ioe);
                            }
                        }));
                    }
                }
                try {
                    CompletableFuture<Void> mergedFilesWait = CompletableFuture.allOf(mergedFiles.toArray(new CompletableFuture[mergedFiles.size()]));
                    mergedFilesWait.get();
                    files.clear();
                    for( CompletableFuture<File> c : mergedFiles ) {
                        files.add( c.get() );
                    }
                } catch (ExecutionException | InterruptedException e) {
                    throw new IOException(e);
                }
            } while (files.size() > 1);
            return files.stream().findFirst().get();
        } finally {
            workers.shutdown();
        }
    }

    private File mergeFiles(File a, File b) throws IOException {
        CSVFile csvA = new CSVFile( a, "," );
        CSVFile csvB = new CSVFile( b, ",");

        try( CSVPullReader readerA = csvA.open();
             CSVPullReader readerB = csvB.open();
             CSVFile mergedFile = new CSVFile( new File( tmpDir, getMergeFileName( a, b ) ), "," ); ) {
            List<String> currentRowA = readerA.nextRow(), currentRowB = readerB.nextRow();
            boolean done = false;
            initRowComparator(csvA.getColumnHeaders());
            do {
                if( currentRowA == null ) {
                    currentRowA = readerA.nextRow();
                }
                if( currentRowB == null ) {
                    currentRowB = readerB.nextRow();
                }

                if( currentRowA != null || currentRowB != null ) {
                    int value = compare(currentRowA, currentRowB);
                    if (value < 0) {
                        mergedFile.write(currentRowA);
                        currentRowA = null;
                    } else if (value > 0) {
                        mergedFile.write(currentRowB);
                        currentRowB = null;
                    } else {
                        mergedFile.write(currentRowA);
                        mergedFile.write(currentRowB);
                        currentRowA = currentRowB = null;
                    }
                } else {
                    done = true;
                }
            } while( !done );
            return mergedFile.getFile();
        }
    }

    private String getMergeFileName(File a, File b) {
        int comp = a.getName().compareTo( b.getName() );
        if( comp < 0 ) return a.getName();
        return b.getName();
    }

    private void initRowComparator(List<String> columnHeaders) {
        Integer[] columnIndexes = new Integer[ sortByColumns.size() ];
        for( int i = 0; i < sortByColumns.size(); i++ ) {
            columnIndexes[i] = columnHeaders.indexOf( sortByColumns.get(i) );
        }
        rowComparator = (o1,o2) -> {
            for( int col : columnIndexes ) {
                int comp = o1.get(col).compareTo( o2.get(col) );
                if( comp != 0 ) return comp;
            }
            return 0;
        };
    }

    private int compare(List<String> currentRowA, List<String> currentRowB) {
        // todo how to figure out sorting by comparables generically rather than just Strings,
        //  we will have to convert Dates, Numbers, etc.
        return rowComparator.compare( currentRowA, currentRowB );
    }

    private class MergeFilesWorker implements Callable<File> {

        File leftFile;
        File rightFile;

        public MergeFilesWorker(File a, File b) {
            this.leftFile = a;
            this.rightFile = b;
        }

        @Override
        public File call() throws Exception {
            return mergeFiles( leftFile, rightFile );
        }
    }
}
