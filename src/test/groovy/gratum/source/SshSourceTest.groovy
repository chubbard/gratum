package gratum.source

import gratum.etl.LoadStatistic
import org.apache.commons.io.IOUtils
import org.junit.Ignore
import org.junit.Test

class SshSourceTest {

    @Test
    @Ignore
    void testSsh() {
        String host = "uknown.host.com"
        String user = "user1"
        String password = "DontHardCodeIt"
        String path = "/some/dir/path.txt"

        File tmp = File.createTempFile("ulti-test", ".txt")
        File knownHosts = File.createTempFile("known_hosts", ".txt")
        knownHosts.withPrintWriter { it.println("output from ssh-keyscan -H ${host}") }
        try {
            tmp.withOutputStream { OutputStream out ->
                LoadStatistic stats = SshSource.ssh(host)
                        .authPass(user, password)
                        .knownHosts( knownHosts )
                        .download(path)
                        .into()
                        .addStep("Download") { Map row ->
                            assert row.filename == path.substring( path.lastIndexOf("/") + 1)
                            assert row.host == host
                            assert row.stream != null
                            try {
                                int length = IOUtils.copy(row.stream as InputStream, out)
                                //assert tmp.length() == length
                                return row
                            } finally {
                                row.stream.close()
                            }
                        }
                        .go()
                println( stats )
            }
        } finally {
            tmp.delete()
            knownHosts.delete()
        }
    }
}
