package gratum.pgp;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.bcpg.ArmoredInputStream;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.operator.jcajce.*;
import org.bouncycastle.util.io.Streams;

import java.io.*;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class PgpContext {

    private PGPSecretKeyRingCollection secretKeys;
    private Iterable<String> identities;
    private boolean asciiArmour = true;
    private boolean checkIntegrity = true;
    private int compressedDataType = PGPCompressedData.ZIP;
    private boolean overwrite = false;

    public PgpContext() {
        Provider provider = Security.getProvider("BC");
        if (provider == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    public PgpContext addKeys( File keyRing ) throws IOException, PGPException {
        try(InputStream stream = new ArmoredInputStream(new FileInputStream(keyRing))) {
            this.secretKeys = new PGPSecretKeyRingCollection(stream, new JcaKeyFingerprintCalculator());
            return this;
        }
    }

    public PgpContext identities( Iterable<String> identities ) {
        this.identities = identities;
        return this;
    }

    public PgpContext checkIntegrity(boolean check) {
        this.checkIntegrity = check;
        return this;
    }

    public PgpContext asciiArmour(boolean asciiArmour) {
        this.asciiArmour = asciiArmour;
        return this;
    }

    public PgpContext compressData( int compressedDataType ) {
        this.compressedDataType = compressedDataType;
        return this;
    }

    public PgpContext overwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public final void encrypt(String filename, Date modificationTime, InputStream stream, OutputStream out) throws IOException, PGPException {
        List<PGPPublicKey> keys = getPgpPublicKeys();
        try {
            if (asciiArmour) {
                out = new ArmoredOutputStream(out);
            }
            JcePGPDataEncryptorBuilder pgpBuilder = new JcePGPDataEncryptorBuilder(PGPEncryptedData.AES_256)
                    .setWithIntegrityPacket(checkIntegrity)
                    .setSecureRandom(new SecureRandom())
                    .setProvider("BC");
            PGPEncryptedDataGenerator encryptedDataGenerator = new PGPEncryptedDataGenerator(pgpBuilder);
            keys.forEach((PGPPublicKey key) -> {
                encryptedDataGenerator.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(key).setProvider("BC"));
            });

            try(OutputStream encryptedOut = encryptedDataGenerator.open(out, new byte[1<<16]) ) {
                PGPCompressedDataGenerator compressedDataGenerator = new PGPCompressedDataGenerator(compressedDataType);
                try( OutputStream compressedOut = compressedDataGenerator.open(encryptedOut, new byte[1 << 16]) ) {
                    PGPLiteralDataGenerator lData = new PGPLiteralDataGenerator();
                    try (OutputStream pgpOut = lData.open(compressedOut, PGPLiteralData.BINARY, filename, modificationTime, new byte[1 << 16])) {
                        IOUtils.copy(stream, pgpOut);
                    }
                }
            }
        } finally {
            out.close();
        }
    }

    private List<PGPPublicKey> getPgpPublicKeys() throws PGPException {
        List<PGPPublicKey> keys = new ArrayList<>();
        for( String identity : identities ) {
            Iterator<PGPSecretKeyRing> rings = secretKeys.getKeyRings(identity, true, true);
            if( !rings.hasNext() ) throw new IllegalArgumentException("No keys for identity: " + identity );
            PGPSecretKeyRing ring = rings.next();
            keys.add( getEncryptionKey(ring) );
        }
        return keys;
    }

    private PGPPublicKey getEncryptionKey(PGPSecretKeyRing ring) {
        Iterator<PGPPublicKey> i = ring.getPublicKeys();
        while (i.hasNext()) {
            PGPPublicKey key = i.next();
            if (key.isEncryptionKey()) {
                return key;
            }
        }
        // no encryption keys found!
        throw new IllegalArgumentException("No encryption keys were found.");
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void decrypt(InputStream inputStream, OutputStream outputStream, char[] passphrase ) throws IOException, PGPException {
        new PgpObjectProcessor(this, passphrase ).onData( (PGPLiteralData pgpLiteralData) -> {
            Streams.pipeAll(pgpLiteralData.getInputStream(), outputStream);
            outputStream.flush();
            return true;
        }).process( inputStream );
    }

    public PGPSecretKey getSecretKey(long keyID) throws PGPException {
        return secretKeys.getSecretKey(keyID);
    }
}
