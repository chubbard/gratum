package gratum.pgp;

import org.bouncycastle.bcpg.HashAlgorithmTags;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.spec.ElGamalParameterSpec;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.operator.PGPDigestCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPDigestCalculatorProviderBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPKeyPair;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyEncryptorBuilder;

import java.security.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class PgpKeyBuilder {

    private String name;
    private String email;
    private char[] passphrase;
    private Date expiration = new Date(System.currentTimeMillis() + TimeUnit.DAYS.convert(365, TimeUnit.MILLISECONDS));
    private int dsaKeySize = 1024;
    private ElGamalKeySize elGamalKeySize = ElGamalKeySize.BIT_4096;

    public PgpKeyBuilder() {
        Provider provider = Security.getProvider("BC");
        if (provider == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    public static PgpKeyBuilder identity(String name, String email, char[] passphrase ) {
        PgpKeyBuilder builder = new PgpKeyBuilder();
        builder.name = name;
        builder.email = email;
        builder.passphrase = passphrase;
        return builder;
    }

    public PgpKeyBuilder expires(Date expiration) {
        this.expiration = expiration;
        return this;
    }

    protected final KeyPair generateDsaKeyPair(int keySize) throws NoSuchAlgorithmException, NoSuchProviderException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("DSA", "BC");
        keyPairGenerator.initialize(keySize);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        return keyPair;
    }

    /**
     * @param keySize - 1024, 2048, 4096
     * @return the El Gamal generated key pair
     * @throws InvalidAlgorithmParameterException algorithm's parameters aren't correct (parameters are supported, etc).
     * @throws NoSuchAlgorithmException algorithm provided does not exist
     * @throws NoSuchProviderException Cannot find BouncyCastle provider which means something isn't installed correctly.
     */
    protected final KeyPair generateElGamalKeyPair(ElGamalKeySize keySize) throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, NoSuchProviderException {
        return generateElGamalKeyPair(keySize.getElGamalParameterSpec());
    }

    /**
     * @param paramSpecs - the pre-defined parameter specs
     * @return the El Gamal generated key pair
     * @throws InvalidAlgorithmParameterException algorithm's parameters aren't correct (parameters are supported, etc).
     * @throws NoSuchAlgorithmException algorithm provided does not exist
     * @throws NoSuchProviderException Cannot find BouncyCastle provider which means something isn't installed correctly.
     */
    protected final KeyPair generateElGamalKeyPair(ElGamalParameterSpec paramSpecs) throws NoSuchProviderException, NoSuchAlgorithmException, InvalidAlgorithmParameterException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("ELGAMAL", "BC");
        keyPairGenerator.initialize(paramSpecs);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        return keyPair;
    }

    public PGPSecretKeyRing build() throws PGPException, NoSuchProviderException, NoSuchAlgorithmException, InvalidAlgorithmParameterException {
        KeyPair dsa = generateDsaKeyPair(dsaKeySize);
        KeyPair elGamal = generateElGamalKeyPair(elGamalKeySize);
        PGPKeyRingGenerator generator = createKeyGenerator( name, email, passphrase, dsa, elGamal );

        return generator.generateSecretKeyRing();
    }

    public PGPKeyRingGenerator createKeyGenerator(String name, String email, char[] passphrase, KeyPair dsaKeyPair, KeyPair elGamalKeyPair) throws PGPException {
        Date now = new Date();

        PGPKeyPair dsaPgpKeyPair = new JcaPGPKeyPair(PGPPublicKey.DSA, dsaKeyPair, now);
        PGPKeyPair elGamalPgpKeyPair = new JcaPGPKeyPair(PGPPublicKey.ELGAMAL_ENCRYPT, elGamalKeyPair, now);
        PGPDigestCalculator sha1Calc = new JcaPGPDigestCalculatorProviderBuilder().build().get(HashAlgorithmTags.SHA1);

        PGPSignatureSubpacketGenerator subpacketGenerator = new PGPSignatureSubpacketGenerator();
        long seconds = expiration != null ? TimeUnit.MILLISECONDS.toSeconds(expiration.getTime() - now.getTime()) : TimeUnit.DAYS.toSeconds(365);
        subpacketGenerator.setKeyExpirationTime(false, seconds);
        PGPSignatureSubpacketVector subpacketVector = subpacketGenerator.generate();

        PGPKeyRingGenerator keyRingGen = new PGPKeyRingGenerator(
                PGPSignature.POSITIVE_CERTIFICATION,
                dsaPgpKeyPair,
                name + " <" + email + ">",
                sha1Calc,
                subpacketVector,
                null,
                new JcaPGPContentSignerBuilder(dsaPgpKeyPair.getPublicKey().getAlgorithm(), HashAlgorithmTags.SHA1),
                new JcePBESecretKeyEncryptorBuilder(PGPEncryptedData.AES_256, sha1Calc).setProvider("BC").build(passphrase)
        );

        keyRingGen.addSubKey(elGamalPgpKeyPair);
        return keyRingGen;
    }
}
