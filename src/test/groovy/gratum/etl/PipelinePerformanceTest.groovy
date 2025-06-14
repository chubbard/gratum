package gratum.etl

import org.junit.Ignore
import org.junit.Test

import static gratum.source.CsvSource.csv

class PipelinePerformanceTest {

    @Test
    @Ignore
    void performanceTest() {
        File tmpFilePw = File.createTempFile("pfchangs", "pw.csv")
        File tmpFileNotPw = File.createTempFile("pfchangs", "not_pw.csv")

        try {
            File perfFile = new File("${System.getProperty("user.home")}/Documents/customer/pfchangs/src/2012/PFC1000_XLodPEarHist_20210207_1512.txt")
            if( perfFile.exists() ) {
                LoadStatistic stat = csv(perfFile, "|")
                        .branch([PehEELink: { it.startsWith("PW") }]) { Pipeline p ->
                            return p.save(tmpFilePw, "|")
                        }.branch([PehEELink: { !it.startsWith("PW") }]) { Pipeline p ->
                            return p.save(tmpFileNotPw, "|")
                        }
                        .go()
                println( stat )
            }
        } finally {
            tmpFileNotPw.delete();
            tmpFilePw.delete();
        }
    }
}
