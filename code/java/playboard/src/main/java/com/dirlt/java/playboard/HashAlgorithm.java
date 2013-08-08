package com.dirlt.java.playboard;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 8/8/13
 * Time: 11:10 AM
 * To change this template use File | Settings | File Templates.
 */
public class HashAlgorithm {
    public static long hash(String umid) {
        byte[] values = umid.toLowerCase().getBytes();
        int code = 0;
        for (byte b : values) {
            int v = (int) b;
            if (v >= (int) '0' && v <= (int) '9') {
                v -= (int) '0';
            } else if (v >= (int) 'a' && v <= (int) 'f') {
                v -= (int) 'a';
                v += 10;
            }
            code = (code * 5) + v;
        }
        return (code & 0x0ffffffffL);
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
        int max_zeros[] = new int[1 << 8];
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String umid = reader.readLine();
            if (umid == null) {
                break;
            }
            long h = hash(umid);
            int bucket = (int) (h % max_zeros.length);
            int bucket_hash = (int) (h / max_zeros.length);
            max_zeros[bucket] = Math.max(max_zeros[bucket], trailing_zero_count(bucket_hash));
        }
        for (int i = 0; i < max_zeros.length; i++) {
            System.out.print(Integer.toString(max_zeros[i]));
            System.out.print(" ");
        }
        System.out.println("");
    }
}
