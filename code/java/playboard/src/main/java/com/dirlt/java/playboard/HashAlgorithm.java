package com.dirlt.java.playboard;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 8/8/13
 * Time: 11:10 AM
 * To change this template use File | Settings | File Templates.
 */
public class HashAlgorithm {
    public static int hash(String umid) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return 0;
        }
        digest.update(umid.getBytes());
        byte[] values = digest.digest();
        int code = 0;
        for (byte b : values) {
            code = (code << 8) + (int) b;
        }
        if (code < 0) {
            code = -code;
        }
        return code;
    }

    public static int trailing_zero_count(int value) {
        if (value == 0) {
            return 32;
        }
        int p = 0;
        while (((value >> p) & 1) == 0) {
            p += 1;
        }
        return p;
    }

    public static void main(String[] args) throws Exception {
        args = "hello world yes no fuck".split(" ");
        for (String arg : args) {
            System.out.println("'" + arg + "' hash code = " + hash(arg));
            System.out.println(hash(arg) >> 8);
        }
    }
}
