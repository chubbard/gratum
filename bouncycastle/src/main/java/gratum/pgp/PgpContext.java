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
import java.util.*;

public class PgpContext {

    private PGPSecretKeyRingCollection secretKeys;
    private PGPPublicKeyRingCollection publicKeys;
    private Iterable<String> identities;
    private char[] passphrase;
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

    public PgpContext addPublicKeys( PGPPublicKeyRingCollection keys ) {
        this.publicKeys = keys;
        return this;
    }

    public PgpContext addSecretKeys( PGPSecretKeyRingCollection keys ) {
        this.secretKeys = keys;
        return this;
    }

    public PgpContext addPublicKeys( File publicKeyRing ) throws IOException, PGPException {
        try(InputStream stream = new ArmoredInputStream(new FileInputStream(publicKeyRing))) {
            if( this.publicKeys == null ) {
                this.publicKeys = new PGPPublicKeyRingCollection(stream, new JcaKeyFingerprintCalculator());
            } else {
                PGPPublicKeyRing keyRing = new PGPPublicKeyRing( stream, new JcaKeyFingerprintCalculator());
                this.publicKeys = PGPPublicKeyRingCollection.addPublicKeyRing( this.publicKeys, keyRing );
            }
            return this;
        }
    }

    public PgpContext addSecretKeys( File secretKeyRing ) throws IOException, PGPException {
        try(InputStream stream = new ArmoredInputStream(new FileInputStream(secretKeyRing))) {
            if( this.secretKeys == null ) {
                this.secretKeys = new PGPSecretKeyRingCollection(stream, new JcaKeyFingerprintCalculator());
            } else {
                PGPSecretKeyRing keyRing = new PGPSecretKeyRing( stream, new JcaKeyFingerprintCalculator());
                this.secretKeys = PGPSecretKeyRingCollection.addSecretKeyRing( this.secretKeys, keyRing );
            }
            return this;
        }
    }

    public PgpContext identity( String identity, char[] passphrase ) {
        this.identities = Collections.singleton( identity );
        this.passphrase = passphrase;
        return this;
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
        assert secretKeys != null || publicKeys != null : "You must provide either a secret key ring or public key ring using addPublicKeys() or addSecretKeys()";
        assert identities != null : "You must have at least 1 identity configured using addIdentities()";

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
            PGPPublicKey key;
            if( (key = getPublicIdentity( identity ) ) != null ) {
                keys.add(key);
            }  else if( (key = getSecretIdentity( identity )) != null ) {
                keys.add(key);
            } else {
                throw new IllegalArgumentException("No keys for identity: " + identity );
            }
        }
        return keys;
    }

    private PGPPublicKey getPublicIdentity(String identity) throws PGPException {
        if( publicKeys == null ) return null;
        Iterator<PGPPublicKeyRing> rings = publicKeys.getKeyRings(identity, true, true);
        if (rings.hasNext()) {
            PGPPublicKeyRing ring = rings.next();
            return getEncryptionKey(ring);
        }
        return null;
    }

    private PGPPublicKey getSecretIdentity( String identity ) throws PGPException {
        if( secretKeys == null ) return null;
        Iterator<PGPSecretKeyRing> rings = secretKeys.getKeyRings(identity, true, true);
        if (rings.hasNext()) {
            PGPSecretKeyRing ring = rings.next();
            return getEncryptionKey(ring);
        }
        return null;
    }

    private PGPPublicKey getEncryptionKey(PGPKeyRing ring) {
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

    public void decrypt(InputStream inputStream, OutputStream outputStream ) throws IOException, PGPException {
        assert secretKeys != null : "You must provide a secret key ring using addSecretKeys()";
        assert identities != null : "You must provide an identity to decrypt using addIdentity()";
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
