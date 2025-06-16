package gratum.csv;

import junit.framework.TestCase;
import org.junit.Ignore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;

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

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] {
                "Charles \"Pinky\" Williams",
                new Date(),
                "I want to voice my opinion about the following things:\n1. Blah blah blah\n2.Blah Blah Blek\n3.Blah Blah Ahhhhhh Rah\nThank you!"
        });
        rows.add(new Object[] {
                "Jill \"The Thrill\" Ryan",
                "",
                "I wish there were more family options"
        });
        rows.add( new Object[] {
                "William \"Bill\" Taylor",
                null,
                "I would like more salad options."
        });

        csv.setColumnHeaders(header);
        csv.write( header.toArray() );
        rows.forEach( (s) -> {
            try {
                csv.write(s);
            } catch (IOException e) {
                throw new RuntimeException( e );
            }
        } );
        csv.flush();
        csv.close();

        String output = writer.toString();
        assertTrue( output.contains("\"name\"|\"date\"|\"comment\"") );
        assertTrue( output.contains("Charles \"\"Pinky\"\" Williams") );
        assertTrue( output.contains("Ryan\"||")); // verify that it wrote out nothing for a blank string
        assertTrue( output.contains("Taylor\"||")); // verify that it wrote out nothing for a null
        assertTrue( output.contains("\\n") );
    }

    public void testWithoutEscaping() throws IOException {
        Reader reader = new InputStreamReader( CSVFileTest.class.getResourceAsStream("/unescaped.csv") );
        CSVFile csv = new CSVFile(reader, "|");
        csv.setEscaped(false);
        csv.parse(new CSVReader() {
            int line = 1;
            @Override
            public void processHeaders(List<String> header) throws Exception {
                assertEquals( 55, header.size() );
                assertEquals( "ConRecType", header.get(0));
                assertEquals( "ConWorkNumber", header.get( header.size() - 1 ));
            }

            @Override
            public boolean processRow(List<String> header, List<String> row) throws Exception {
                int index;
//                assertEquals( 55, row.size() );
                switch( line ) {
                    case 1:
                        index = header.indexOf("ConNameFirst");
                        assertFalse(row.get(index).contains("\""));
                        assertEquals( "martini", row.get(index).trim() );
                        break;
                    case 2:
                        index = header.indexOf("ConNameFirst");
                        assertTrue( row.get(index).contains("\"") );
                        assertEquals( "La\"Quint", row.get(index).trim() );
                        break;
                    case 3:
                        index = header.indexOf("ConNameLast");
                        assertTrue( row.get(index).contains("\"") );
                        assertEquals( row.get(index).trim(), "o\"neill" );
                        break;
                }

                line++;
                return false;
            }

            @Override
            public void afterProcessing() {
                assertEquals( 4, line );
            }
        });
    }

    public void testReadUnicode() throws IOException {
        File tmp = writeTestUnicodeFile();
        try {
            CSVFile csv = new CSVFile( tmp, ",");

            csv.parse(new CSVReader() {

                int lines = 0;
                @Override
                public void processHeaders(List<String> header) throws Exception {
                }

                @Override
                public boolean processRow(List<String> header, List<String> row) throws Exception {
                    lines++;
                    assertTrue("Line contains an \u00e9", row.get(0).contains("\u00e9"));
                    return false;
                }

                @Override
                public void afterProcessing() {
                    assertEquals(2, lines);
                }
            });
        } finally {
            tmp.delete();
        }
    }

    public void testWriteUnicode() throws IOException {
        File tmp = writeTestUnicodeFile();
        try {
//            try( BufferedReader reader = new BufferedReader( new FileReader(tmp) ) ) {
            try( BufferedReader reader = new BufferedReader( new InputStreamReader( new FileInputStream(tmp), StandardCharsets.UTF_8) ) ) {
                String line;
                int n = 1;
                while( (line = reader.readLine()) != null ) {
                    if( n > 1 ) {
                        assertTrue("Line contains a \u00e9", line.contains("\u00e9"));
                    }
                    n++;
                }
            }
        } finally {
            tmp.delete();
        }
    }

    public void testLastColumnMissing() throws IOException {
        CSVFile csv = new CSVFile( new InputStreamReader(getClass().getResourceAsStream("/empty_last_column_test.csv")), "," );
        csv.parse(new CSVReader() {
            int line = 1;
            @Override
            public void processHeaders(List<String> header) throws Exception {
                assertEquals(5, header.size());
            }

            @Override
            public boolean processRow(List<String> header, List<String> row) throws Exception {
                assertEquals( "line " + line, header.size(), row.size() );
                line++;
                return false;
            }
        });
    }

    public void testUnescapedCsvTailingSeparator() throws IOException {
        CSVFile csv = new CSVFile( new InputStreamReader(getClass().getResourceAsStream("/empty_last_column_test.csv")), "," );
        csv.setEscaped(false);
        csv.parse(new CSVReader() {
            int line = 1;
            @Override
            public void processHeaders(List<String> header) throws Exception {
                assertEquals(5, header.size());
            }

            @Override
            public boolean processRow(List<String> header, List<String> row) throws Exception {
                assertEquals( "line " + line, header.size(), row.size() );
                line++;
                return false;
            }
        });
    }

    private File writeTestUnicodeFile() throws IOException {
        Map<String,Object> person1 =new HashMap<>();
        person1.put("Name", "Andri\u00e9");

        Map<String,Object> person2 =new HashMap<>();
        person2.put("Name", "Ren\u00e9e");

        File tmp = File.createTempFile("testWriteUnicode", ".csv");
        CSVFile file = new CSVFile(tmp, ",");
        try {
            file.write(person1);
            file.write(person2);
        } finally {
            file.close();
        }
        return tmp;
    }
}
