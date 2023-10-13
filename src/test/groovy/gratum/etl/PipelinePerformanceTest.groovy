package gratum.etl

import org.junit.Ignore
import org.junit.Test

import static gratum.source.CsvSource.csv

class PipelinePerformanceTest {

    @Test
    @Ignore
    void performanceTest() {
        File tmpFilePw = File.createTempFile("pfc", "pw.csv")
        File tmpFileNotPw = File.createTempFile("pfc", "not_pw.csv")

        try {
            File perfFile = new File("${System.getProperty("user.home")}/Documents/customer/pfchangs/src/2020/PFC1000_XLodEEErn_20210205_1030.txt")
            assert perfFile.exists()
            LoadStatistic stat = csv(perfFile, "|")
                    .branch([EeeEELink: { ((String)it).startsWith("PW") }]) { Pipeline p ->
                        return p.save(tmpFilePw.absolutePath, "|")
                    }.branch([EeeEELink: { !((String)it).startsWith("PW") }]) { Pipeline p ->
                        return p.save(tmpFileNotPw.absolutePath, "|")
                    }
                    .go()
            println( stat )

            assert tmpFileNotPw.exists()
            assert tmpFileNotPw.size() > 0L
            assert tmpFilePw.exists()
            assert tmpFilePw.size() > 0L
        } finally {
            tmpFileNotPw.delete();
            tmpFilePw.delete();
        }
    }
}
