package gratum.pgp;

import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPDigestCalculatorProviderBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyDataDecryptorFactoryBuilder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PgpObjectProcessor {

    private PgpContext context;
    Map<Class<?>, PgpObjectCallback> callbacks = new HashMap<>();

    public PgpObjectProcessor(PgpContext context, char[] passphrase) {
        this.context = context;
        onEncryptedData((PGPEncryptedDataList data) -> {
            Iterator<PGPEncryptedData> it = data.getEncryptedDataObjects();

            PGPPublicKeyEncryptedData publicKeyEncryptedData = null;

            boolean processed = false;
            while (!processed && it.hasNext()) {
                publicKeyEncryptedData = (PGPPublicKeyEncryptedData) it.next();
                PGPSecretKey pgpSecKey = context.getSecretKey(publicKeyEncryptedData.getKeyIdentifier().getKeyId());

                if (pgpSecKey != null) {
                    PGPPrivateKey secretKey = pgpSecKey.extractPrivateKey(
                            new JcePBESecretKeyDecryptorBuilder(new JcaPGPDigestCalculatorProviderBuilder().setProvider("BC").build())
                                    .setProvider("BC")
                                    .build(passphrase)
                    );
                    InputStream clear = publicKeyEncryptedData.getDataStream(
                            new JcePublicKeyDataDecryptorFactoryBuilder()
                                    .setProvider("BC")
                                    .build(secretKey)
                    );
                    PGPObjectFactory plainFact = new PGPObjectFactory(clear, new JcaKeyFingerprintCalculator());
                    processed = process(plainFact);

                    if (processed) {
                        if (publicKeyEncryptedData.isIntegrityProtected()) {
                            if (!publicKeyEncryptedData.verify()) {
                                throw new PGPException("Message failed integrity check against key: " + Long.toHexString(publicKeyEncryptedData.getKeyIdentifier().getKeyId()));
                            }
                        }
                    }
                } else {
                    throw new PGPException("Secret key was not found for keyId = " + Long.toHexString(publicKeyEncryptedData.getKeyIdentifier().getKeyId()));
                }
            }
            return processed;
        });

        onCompressed((PGPCompressedData compressedData) -> {
            InputStream compressedStream = new BufferedInputStream(compressedData.getDataStream());
            PGPObjectFactory pgpFact = new PGPObjectFactory(compressedStream, new JcaKeyFingerprintCalculator());
            return process(pgpFact);
        });
    }

    public boolean process(InputStream encryptedStream) throws IOException, PGPException {
        InputStream in = PGPUtil.getDecoderStream(encryptedStream);
        PGPObjectFactory pgpObjFactory = new PGPObjectFactory(in, new JcaKeyFingerprintCalculator());
        return process(pgpObjFactory);
    }

    protected boolean process(PGPObjectFactory factory) throws PGPException, IOException {
        boolean processed = false;
        Object pgpObj;
        while ((pgpObj = factory.nextObject()) != null) {
            if (callbacks.containsKey(pgpObj.getClass())) {
                processed = callbacks.get(pgpObj.getClass()).handle(pgpObj);
            }
        }
        return processed;
    }

    public PgpObjectProcessor onData(PgpObjectCallback<PGPLiteralData> callback) {
        callbacks.put(PGPLiteralData.class, callback);
        return this;
    }

    public PgpObjectProcessor onCompressed(PgpObjectCallback<PGPCompressedData> callback) {
        callbacks.put(PGPCompressedData.class, callback);
        return this;
    }

    public PgpObjectProcessor onSignature(PgpObjectCallback<PGPSignatureList> callback) {
        callbacks.put(PGPSignatureList.class, callback);
        return this;
    }

    public PgpObjectProcessor onOnePassSignature(PgpObjectCallback<PGPOnePassSignatureList> callback) {
        callbacks.put(PGPOnePassSignatureList.class, callback);
        return this;
    }

    public PgpObjectProcessor onEncryptedData(PgpObjectCallback<PGPEncryptedDataList> callback) {
        callbacks.put(PGPEncryptedDataList.class, callback);
        return this;
    }

    public PgpObjectProcessor onSecretKeyRing(PgpObjectCallback<PGPSecretKeyRing> callback) {
        callbacks.put(PGPSecretKeyRing.class, callback);
        return this;
    }

    public PgpObjectProcessor onPublicKeyRing(PgpObjectCallback<PGPPublicKeyRing> callback) {
        callbacks.put(PGPPublicKeyRing.class, callback);
        return this;
    }
}
