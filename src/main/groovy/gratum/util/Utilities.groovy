package gratum.util

import org.apache.commons.codec.binary.Hex

import java.nio.charset.Charset
import java.security.MessageDigest

/**
 * Created by charliehubbard on 7/12/18.
 */
class Utilities {
    public static String MD5(String source) {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.reset();
        messageDigest.update(source.getBytes(Charset.forName("UTF8")));
        final byte[] resultByte = messageDigest.digest();
        return new String(Hex.encodeHex(resultByte));
    }
}
