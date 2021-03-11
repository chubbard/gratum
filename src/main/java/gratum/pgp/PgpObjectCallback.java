package gratum.pgp;

import org.bouncycastle.openpgp.PGPException;

import java.io.IOException;

public interface PgpObjectCallback<T> {
    public boolean handle(T t) throws PGPException, IOException;
}
