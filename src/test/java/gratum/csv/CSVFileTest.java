package gratum.csv;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by charlie on 8/16/15.
 */
public class CSVFileTest {

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
        Reader reader = new InputStreamReader(getResourceAsStream("unescaped.csv"));
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
                    assertTrue(row.get(0).contains("\u00e9"), "Line does not contain an \u00e9");
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
                        assertTrue(line.contains("\u00e9"), "Line does not contain a \u00e9");
                    }
                    n++;
                }
            }
        } finally {
            tmp.delete();
        }
    }

    public void testLastColumnMissing() throws IOException {
        CSVFile csv = new CSVFile( new InputStreamReader(getResourceAsStream("empty_last_column_test.csv")), "," );
        csv.parse(new CSVReader() {
            int line = 1;
            @Override
            public void processHeaders(List<String> header) throws Exception {
                assertEquals(5, header.size());
            }

            @Override
            public boolean processRow(List<String> header, List<String> row) throws Exception {
                assertEquals(header.size(), row.size(),  "line " + line);
                line++;
                return false;
            }
        });
    }

    public void testUnescapedCsvTailingSeparator() throws IOException {
        CSVFile csv = new CSVFile( new InputStreamReader(getResourceAsStream("empty_last_column_test.csv")), "," );
        csv.setEscaped(false);
        csv.parse(new CSVReader() {
            int line = 1;
            @Override
            public void processHeaders(List<String> header) throws Exception {
                assertEquals(5, header.size());
            }

            @Override
            public boolean processRow(List<String> header, List<String> row) throws Exception {
                assertEquals(header.size(), row.size(),  "line " + line);
                line++;
                return false;
            }
        });
    }

    @NotNull
    private static InputStream getResourceAsStream(String name) {
        return Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(name));
    }

    public void testMultilineRows() throws IOException {
        CSVFile csv = new CSVFile(text(
                "personId,comment",
                "1,\"This is a multi-line comment.\nIt could be something more\n,but we decided to just test\nhaving multiple lines to parse.\"",
                "2,\"This is not multi-line comment.  We needed at least one that didn't have extra lines.\\n But we did try escaping a newline just to test combining our methods.\"",
                "3, \"This is not multi-line without escaping.  We needed just one.\"",
                "4,\"This is a multi-line without escaping, but contains a \\r at the end like windows.\r\\nDoes this parse ok?\""
        ), ",");

        csv.parse(new CSVReader() {
            int lineNumber = 0;

            @Override
            public void processHeaders(List<String> header) throws Exception {
                assertTrue( header.contains("personId"), "Assert personId is present" );
                assertTrue( header.contains("comment"), "Assert comment is present" );
            }

            @Override
            public boolean processRow(List<String> header, List<String> row) throws Exception {
                String personId = row.get(0);
                String comment = row.get(1);

                assertTrue(personId != null && !personId.isEmpty(), "Assert personId is present");
                assertTrue(comment != null && !comment.isEmpty(), "Assert comment is present");
                if( lineNumber == 0 ) {
                    assertTrue(comment.contains("This is a multi-line comment."), "Assert that '" + comment + "' contains 1st line");
                    assertTrue(comment.contains("It could be something more"), "Assert that '" + comment + "' contains 2nd line");
                    assertTrue(comment.contains(",but we decided to just test"), "Assert that '" + comment + "' comment contains 3rd line");
                    assertTrue(comment.contains("having multiple lines to parse."), "Assert that '" + comment + "' comment contains 3rd line");
                } else if( lineNumber == 1 ) {
                    assertTrue(comment.contains("This is not multi-line comment.  We needed at least one that didn't have extra lines.\n But we did try escaping a newline just to test combining our methods."), "Assert that '" + comment + "' contains the whole line");
                } else if( lineNumber == 2 ) {
                    assertTrue(comment.contains("This is not multi-line without escaping.  We needed just one."), "Assert that '" + comment + "' contains the whole line");
                }else if( lineNumber == 3 ) {
                    assertTrue(comment.contains("This is a multi-line without escaping, but contains a \\r at the end like windows.\n\nDoes this parse ok?"), "Assert that '" + comment + "' contains the whole line with \\r");
                }
                lineNumber++;
                return false;
            }
        });
    }

    private Reader text(String... txt) {
        return new StringReader(String.join("\n", txt));
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
