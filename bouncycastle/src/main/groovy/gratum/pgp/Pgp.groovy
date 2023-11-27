package gratum.pgp

import gratum.etl.Pipeline
import gratum.pgp.PgpContext
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import gratum.etl.*

public class Pgp {

    /**
     * Encrypts using PGP a stream on the pipeline and rewrite that stream back to the pipeline.  It looks for
     * a stream on the Pipeline at streamProperty. Further configuration is performed by the provided Closure
     * that is passed a {@link gratum.pgp.PpgContext}.  You are required to setup the identities, secret key collection,
     * and/or public key collection in order to encrypt.  This will write the encrypted stream back to the Pipeline
     * on the provided streamProperty.  It also adds the file and filename properties to the existing row.
     * @param streamProperty The property that holds a stream object to be encrypted.
     * @param configure The Closure that is passed the PgpContext used to configure how the stream will be encrypted.
     */
    static Closure<Pipeline> encryptPgp(String streamProperty,
                                @ClosureParams( value = FromString, options = ["gratum.pgp.PgpContext"])
                                Closure configure ) {
        return { Pipeline pipeline ->
            PgpContext pgp = new PgpContext()
            configure.call( pgp )
            return pipeline.addStep("encrypt(${streamProperty})") { Map row ->
                File encryptedTemp = File.createTempFile("pgp-encrypted-output-${streamProperty}".toString(), ".gpg")
                InputStream stream = row[streamProperty] as InputStream
                try {
                    encryptedTemp.withOutputStream { OutputStream out ->
                        pgp.encrypt((String) row.filename, new Date(), stream, out)
                    }
                } finally {
                    stream.close()
                }
                if( pgp.isOverwrite() ) {
                    File f = row?.file as File
                    f?.delete()
                }
                row.file = encryptedTemp
                row.filename = encryptedTemp.getName()
                row[streamProperty] = new FileOpenable(encryptedTemp)
                return row
            }
        }
    }

    /**
     * Decrypts using PGP a stream on the Pipeline and rewrites the stream back onto the Pipeline.  It looks for
     * a stream at the given streamProperty.  Further configuration is performed by the provided Closure
     * that is passed a {@link gratum.pgp.PpgContext}.  You are required to setup the identity passphrase and the secret
     * key collection used to decrypt. It also adds the file and filename properties to the existing row.
     * @param streamProperty The property within the row on the Pipeline that stores a stream.
     * @param configure The closure called with a PgpContext object to further configure how it will decrypt the stream.
     * @return a Pipeline where the streamProperty contains decrypted stream.
     */
    static Closure<Pipeline> decryptPgp(String streamProperty,
                                        @ClosureParams( value = FromString, options = ["gratum.pgp.PgpContext"])
                               Closure configure ) {
        return { Pipeline pipeline ->
            PgpContext pgp = new PgpContext()
            configure.call( pgp )
            return pipeline.addStep("decrypt(${streamProperty})") { Map row ->
                InputStream stream = row[streamProperty] as InputStream
                File decryptedFile = File.createTempFile("pgp-decrypted-output-${streamProperty}", "out")
                try {
                    decryptedFile.withOutputStream { OutputStream out ->
                        pgp.decrypt( stream, out )
                    }
                } finally {
                    stream.close()
                }

                // todo should we get the original file name??!!
                row.file = decryptedFile
                row.filename = decryptedFile.name
                row[streamProperty] = new FileOpenable( decryptedFile )
                return row
            }
        }
    }
}