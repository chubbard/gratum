package gratum.source

import gratum.etl.GratumFixture
import gratum.etl.LoadStatistic
import org.junit.jupiter.api.Test

import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

class XlsxSourceTest {

    @Test
    void testXlsxLoading() {
        int id = 1
        LoadStatistic stat = XlsxSource.xlsx( "Players", GratumFixture.getResource("players.xlsx") ).into()
            .asInt("ID")
            .addStep("Verify") { Map row ->
                assert row.size() == 6 // should have 6 columns
                if( row.ID != 7 ) {
                    row.each { String col, Object value ->
                        assert value != null
                    }
                }
                assert row.ID == id
                id++
                return row
            }
            .go()

        assert stat.loaded == 7
        assert stat.rejections == 0
    }

    @Test
    void testXlsxGroupBy() {
        LoadStatistic stat = XlsxSource.xlsx("Players", GratumFixture.getResource("players.xlsx")).into()
            .groupBy("color")
                    .addStep("Verify groups") { Map<String,List<Map<String,Object>>> row ->
                assert row.size() == 6
                assert row.green.size() == 2
                assert row.blue.size() == 1
                assert row.purple.size() == 1
                return row
            }
            .go()

        assert stat.loaded == 1
        assert stat.rejections == 0
    }

    @Test
    void testXlsxDates() {
        LoadStatistic stat = XlsxSource.xlsx("Players", GratumFixture.getResource("players.xlsx")).into()
            .asDate("birth_date", "yyyy-MM-dd")
            .addStep("Verify Dates") { Map<String,Object> row ->
                assert row.birth_date instanceof Date
                Calendar cal = Calendar.getInstance()
                cal.setTime( row.birth_date )
                assert cal.get( Calendar.YEAR ) > 1900
                return row
            }
            .go()

        assert stat.loaded == 7
        assert stat.rejections == 0
    }

    @Test
    void testXlsxCustomDateFormat() {
        LoadStatistic stat = XlsxSource.xlsx("Players", GratumFixture.getResource("players.xlsx")).dateFormat("MM/dd/yyyy").into()
                .asDate("birth_date", "MM/dd/yyyy")
                .addStep("Verify Dates") { Map<String,Object> row ->
                    assert row.birth_date instanceof Date
                    Calendar cal = Calendar.getInstance()
                    cal.setTime( row.birth_date )
                    assert cal.get( Calendar.YEAR ) > 1900
                    return row
                }
                .go()

        assert stat.loaded == 7
        assert stat.rejections == 0
    }

    @Test
    void testDecryptXlsx() {
        File encryptedFile = File.createTempFile("encrypted_players", ".xlsx")
        GratumFixture.withResource("encrypted_players.xlsx") {
            encryptedFile << it
        }
        try {
            LoadStatistic stat = XlsxSource.xlsx(encryptedFile)
                    .password("Freedom")
                    .dateFormat("MM/dd/yyyy")
                    .into()
                    .asDate("birth_date", "MM/dd/yyyy")
                    .addStep("Verify Dates") { Map<String, Object> row ->
                        assert row.birth_date instanceof Date
                        Calendar cal = Calendar.getInstance()
                        cal.setTime(row.birth_date)
                        assert cal.get(Calendar.YEAR) > 1900
                        return row
                    }
                    .go()

            assert stat.loaded == 7
            assert stat.rejections == 0
        } finally {
            encryptedFile.delete()
        }
    }

    @Test
    void testParsingMissingCellAddressFileTypes() {
        GratumFixture.withResource("players.xlsx") { stream ->
            File stripped = stripCellRefs( stream )
            println XlsxSource.xlsx(stripped, "Sheet 1")
                    .into()
                    .addStep("testParsingMissingCellAddressFileTypes") { row ->
                        def columns = [
                                "ID",
                                "name",
                                "color",
                                "score",
                                "gender",
                                "birth_date"
                        ]
                        columns.each { col ->
                            assert row.containsKey(col)
                            assert row[col]
                        }
                        return row
                    }
                    .go()
        }
    }

    private File stripCellRefs(InputStream stream) {
        File output = File.createTempFile("stripped", ".xlsx")
        ZipInputStream zis = new ZipInputStream( stream )
        new ZipOutputStream( new FileOutputStream(output) ).withStream { zos ->
            ZipEntry entry
            // Iterate through every file inside the .xlsx package
            while ((entry = zis.getNextEntry()) != null) {
                // Prepare a new entry in the destination zip file
                zos.putNextEntry(new ZipEntry(entry.getName()))

                if (entry.getName().startsWith("xl/worksheets/sheet")) {
                    // Read the specific XML worksheet into memory
                    BufferedReader reader = new BufferedReader(new InputStreamReader(zis, "UTF-8"))
                    StringBuilder xmlBuilder = new StringBuilder()
                    String line
                    while ((line = reader.readLine()) != null) {
                        xmlBuilder.append(line).append("\n")
                    }

                    String xmlContent = xmlBuilder.toString()
                    // Strip cell references: r="A1"
                    xmlContent = xmlContent.replaceAll("\\s+r=\"[A-Z0-9]+\"", "")
                    // Strip row references: r="1"
                    xmlContent = xmlContent.replaceAll("<row\\s+r=\"\\d+\"", "<row")

                    // Write the scrubbed XML to the new package
                    byte[] bytes = xmlContent.getBytes("UTF-8")
                    zos.write(bytes, 0, bytes.length)
                } else {
                    // For all other structural files, just pass the raw bytes straight through
                    byte[] buffer = new byte[4096]
                    int len
                    while ((len = zis.read(buffer)) > 0) {
                        zos.write(buffer, 0, len)
                    }
                }
                zos.closeEntry()
                zis.closeEntry()
            }
        }
        return output
    }
}
