package gratum.csv;

import junit.framework.TestCase;

import java.io.*;
import java.util.*;

/**
 * Created by charlie on 8/16/15.
 */
public class CSVFileTest extends TestCase {

    public void testCsvNoQuotes() throws IOException {
        String src = "name,age,birthDate\n"
                + "Tom Hanks,59,7/9/1956\n"
                + "Meg Ryan,54,11/19/1961\n"
                + "Tom Cruise,53,7/3/1962\n";
        CSVFile f = new CSVFile((File)null,",");

        assertCsvDataAsExpected(f, src);
    }

    private void assertCsvDataAsExpected(CSVFile f, String src) throws IOException {
        f.parse( new StringReader(src), new CSVReader() {
            int line = 1;

            @Override
            public void processHeaders(List<String> header) throws Exception {
                assertEquals("name", header.get(0) );
                assertEquals("age", header.get(1) );
                assertEquals("birthDate", header.get(2) );
            }

            @Override
            public boolean processRow(List<String> header, List<String> row) throws Exception {
                if( line == 1 ) {
                    assertEquals("Tom Hanks", row.get(0));
                    assertEquals("59", row.get(1));
                    assertEquals("7/9/1956", row.get(2));
                } else if( line == 2 ) {
                    assertEquals("Meg Ryan", row.get(0));
                    assertEquals("54", row.get(1));
                    assertEquals("11/19/1961", row.get(2));
                } else if( line == 3 ) {
                    assertEquals("Tom Cruise", row.get(0));
                    assertEquals("53", row.get(1));
                    assertEquals("7/3/1962", row.get(2));
                }
                line++;
                return false;
            }

            @Override
            public void afterProcessing() {

            }
        } );
    }

    public void testCsvWithQuotes() throws IOException {
        String src = "\"name\",\"age\",\"birthDate\"\n"
                + "\"Tom Hanks\",\"59\",\"7/9/1956\"\n"
                + "\"Meg Ryan\",\"54\",\"11/19/1961\"\n"
                + "\"Tom Cruise\",\"53\",\"7/3/1962\"\n";
        CSVFile f = new CSVFile((File)null,",");

        assertCsvDataAsExpected(f, src);
    }

    public void testCsvMixedQuotes() throws IOException {
        String src = "name,\"age\",birthDate\n"
                + "Tom Hanks,\"59\",\"7/9/1956\"\n"
                + "\"Meg Ryan\",54,\"11/19/1961\"\n"
                + "Tom Cruise,53,7/3/1962\n";
        CSVFile f = new CSVFile((File)null,",");

        assertCsvDataAsExpected(f, src);
    }

    public void testCsvEscapedQuotes() throws IOException {
        String src = "name,\"age\",birthDate\n"
                + "Tom \"\"Big\"\" Hanks,\"59\",\"7/9/1956\"\n"
                + "\"Meg \"\"Botched\"\" Ryan\",54,\"11/19/1961\"\n"
                + "Tom \"\"Cra-Cra\"\" Cruise,53,7/3/1962\n";
        CSVFile f = new CSVFile((File)null,",");

        f.parse( new StringReader(src), new CSVReader() {
            int line = 1;

            @Override
            public void processHeaders(List<String> header) throws Exception {
                assertEquals("name", header.get(0) );
                assertEquals("age", header.get(1) );
                assertEquals("birthDate", header.get(2) );
            }

            @Override
            public boolean processRow(List<String> header, List<String> row) throws Exception {
                if( line == 1 ) {
                    assertEquals("Tom \"Big\" Hanks", row.get(0));
                    assertEquals("59", row.get(1));
                    assertEquals("7/9/1956", row.get(2));
                } else if( line == 2 ) {
                    assertEquals("Meg \"Botched\" Ryan", row.get(0));
                    assertEquals("54", row.get(1));
                    assertEquals("11/19/1961", row.get(2));
                } else if( line == 3 ) {
                    assertEquals("Tom \"Cra-Cra\" Cruise", row.get(0));
                    assertEquals("53", row.get(1));
                    assertEquals("7/3/1962", row.get(2));
                }
                line++;
                return false;
            }

            @Override
            public void afterProcessing() {

            }
        } );
    }

    public void testReluctantPhrase() throws IOException {
        String src = "\"01-APR-2014\"|\"INV SVC -G\"|\"SD\"|\"CORE SVCS\"|\"\"\"\"|\"\"|\"Investor Services Complex\"|\"Service Delivery\"|\"Core Services\"";
        CSVFile f = new CSVFile((File)null,"|");
        f.setColumnHeaders(Collections.emptyList() );

        f.parse( new StringReader(src), new CSVReader() {
            int line = 1;

            @Override
            public void processHeaders(List<String> header) throws Exception {
                fail("Header should not be called.");
            }

            @Override
            public boolean processRow(List<String> header, List<String> row) throws Exception {
                assertEquals("01-APR-2014", row.get(0));
                assertEquals("INV SVC -G", row.get(1));
                assertEquals("SD", row.get(2));
                assertEquals("CORE SVCS", row.get(3));
                assertEquals("\"", row.get(4));
                assertEquals("", row.get(5));
                assertEquals("Investor Services Complex", row.get(6));
                assertEquals("Service Delivery", row.get(7));
                assertEquals("Core Services", row.get(8));
                return true;
            }

            @Override
            public void afterProcessing() {

            }
        });
    }

    public void testWriteCsv() throws IOException {
        StringWriter writer = new StringWriter();
        PrintWriter out = new PrintWriter( writer );
        CSVFile csv = new CSVFile( out, "|");

        List<String> header = Arrays.asList(
                "name",
                "date",
                "comment"
        );

        Object[] row = new Object[] {
                "Charles \"Pinky\" Williams",
                new Date(),
                "I want to voice my opinion about the following things:\n1. Blah blah blah\n2.Blah Blah Blek\n3.Blah Blah Ahhhhhh Rah\nThank you!"
        };

        csv.setColumnHeaders(header);
        csv.write( header.toArray() );
        csv.write( row );
        csv.flush();
        csv.close();

        String output = writer.toString();
        assertTrue( output.contains("\"name\"|\"date\"|\"comment\"") );
        assertTrue( output.contains("Charles \"\"Pinky\"\" Williams") );
        assertTrue( output.contains("\\n") );
    }
}
