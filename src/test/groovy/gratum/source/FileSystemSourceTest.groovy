package gratum.source

import gratum.etl.LoadStatistic
import org.junit.After
import org.junit.Before
import org.junit.Test

class FileSystemSourceTest {

    File dir
    File csvFile1, csvFile2, textFile1, textFile2

    @Before
    void setUp() {
        dir = File.createTempDir()

        File subdir1 = new File( dir, "dir")
        File subdir2 = new File( dir, "dir2" )

        subdir1.mkdir()
        subdir2.mkdir()

        csvFile1 = File.createTempFile("gratum", ".csv", subdir1)
        csvFile2 = File.createTempFile("gratum", ".csv", subdir2)
        textFile1 = File.createTempFile("gratum", ".txt", subdir1)
        textFile2 = File.createTempFile("gratum", ".txt", subdir2)
    }

    @After
    void cleanUp() {
        dir.deleteDir()
    }

    @Test
    void testFileSource() {
        LoadStatistic stats = FileSystemSource.files( dir ).into().addStep("Assert") { Map row ->
            assert row.file != null
            return row
        }
        .go()

        assert stats.loaded == 4
    }

    @Test
    void testFileSourceWithFilter() {
        LoadStatistic stats = FileSystemSource.files( dir ).filter(~/.*\.csv/).into().addStep("Assert") { Map row ->
            assert row.file != null
            return row
        }
        .go()

        assert stats.loaded == 2
    }
}
