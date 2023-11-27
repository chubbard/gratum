package gratum.etl

import gratum.pgp.*
import org.bouncycastle.bcpg.ArmoredOutputStream
import org.bouncycastle.openpgp.PGPCompressedData
import org.bouncycastle.openpgp.PGPSecretKeyRing
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection
import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Test

import static gratum.source.CollectionSource.from

class PgpTest {

    File secretKeyRingFile

    public static final List<Map> people = [
                [id: 1, name: 'Bill Rhodes', age: 53, gender: 'male', comment: """
I had the single cheese burger.  It was juicy and well seasoned.  The fries 
were on the soggy side and I had to wait for a while to get my milkshake.
"""],
                [id: 2, name: 'Cheryl Lipscome', age: 43, gender: 'female', comment: """
I had the chicken salad.  It was delicious.  I would like more raisins next time.
"""],
                [id: 3, name: 'Diana Rogers', age: 34, gender: 'female', comment: """
I had to wait a very long time for my cheeseburger, and when it came it didn't have
cheese.  I had to send it back, but they got it right the second time.  The burger
was good, and the fries were crispy and warm.
"""],
                [id: 4, name: 'Jack Lowland', age: 25, gender: 'male', comment: """
I loved my burger and milkshake.
"""],
                [id: 5, name: 'Ginger Rogers', age: 83, gender: 'female', comment: """
I had the chili dog and the onion rings, but I wish you had tater tots.
"""]
        ]

    @Before
    void setUp() {
        PGPSecretKeyRing key = PgpKeyBuilder.identity("Sue", "sue@boy.com", "SueIsStillABoy!".getChars()).build();
        PGPSecretKeyRingCollection keyRing = new PGPSecretKeyRingCollection([key])
        secretKeyRingFile = File.createTempFile("testPgpEncryption", "asc")
        secretKeyRingFile.withOutputStream {
            OutputStream asciiOut = new ArmoredOutputStream(it)
            asciiOut.write(keyRing.getEncoded())
            asciiOut.close()
        }

    }

    @After
    void cleanUp() {
        secretKeyRingFile.delete()
    }


    @Test
    public void testPgpEncryption() {
        File tmp = File.createTempFile("pgp-encryption-test", ".gpg")
        try {
            LoadStatistic stat = from(PgpTest.people)
                    .save(tmp.getAbsolutePath())
                    .apply( Pgp.encryptPgp("stream") { PgpContext pgp ->
                        pgp.addSecretKeys( secretKeyRingFile ).identities(["Sue <sue@boy.com>"]).overwrite(true)
                    })
                    .addStep("Test encrypted stream is available") { Map row ->
                        assert row.filename != null
                        assert row.stream != null
                        (row.stream as InputStream).withReader {
                            assert it.readLines().findResult {it.contains("-----BEGIN PGP MESSAGE-----") } != null
                        }
                        row.file.delete()
                        return row
                    }.go()
            assert stat.loaded > 0
            assert stat.rejections == 0
        } finally {
            tmp.delete()
        }
    }

    @Test
    void testPgpDecryption() {
        File tmp = File.createTempFile("pgp-decryption", ".csv")
        try {
            LoadStatistic stat = from(PgpTest.people)
                    .save(tmp.getAbsolutePath())
                    .apply( Pgp.encryptPgp("stream") { PgpContext pgp ->
                        pgp.addSecretKeys( secretKeyRingFile )
                                .identities(["Sue <sue@boy.com>"])
                                .overwrite(false)
                    })
                    .apply( Pgp.decryptPgp("stream") { PgpContext pgp ->
                        pgp.addSecretKeys(secretKeyRingFile).identity("sue@boy.com", "SueIsStillABoy!".getChars())
                    })
                    .addStep("Assert the same") { Map row ->
                        assert tmp.length() == row.file.length()
                        row.file.delete()
                        return row
                    }
                    .go()
        } finally {
            tmp.delete()
        }
    }

    @Test
    @Ignore
    void performanceTest() {
        File perfFile = new File("${System.getProperty("user.home")}/Documents/customer/pfchangs/src/2012/PFC1000_XLodEEDed_20210207_1512.txt")
        LoadStatistic stat = from([file: perfFile, filename: perfFile.name, stream: new FileOpenable(perfFile)])
                .apply( Pgp.encryptPgp("stream") { context ->
                    context.addSecretKeys( secretKeyRingFile )
                            .identities(["Sue <sue@boy.com>"])
                            .compressData(PGPCompressedData.UNCOMPRESSED)
                            .overwrite(false)
                })
                .addStep("Test encrypted stream is available") { Map row ->
                    assert row.filename != null
                    assert row.stream != null
                    (row.stream as InputStream).withReader {
                        assert it.readLine().contains("-----BEGIN PGP MESSAGE-----")
                    }
                    row.file.delete()
                    return row
                }.go()
        assert stat.loaded > 0
        assert stat.rejections == 0
    }

}
