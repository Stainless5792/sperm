package com.dirlt.java.FastHBaseRest;

import java.security.MessageDigest;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 7/1/13
 * Time: 5:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class Utility {
    public static String addHashCodeAsPrefix(String s) throws Exception {
        String hashCode = getMD5(s);
        return hashCode.substring(0, 4) + "_" + s;
    }

    public static String getMD5(String input) throws Exception {
        // Create MD5 Hash
        MessageDigest digest = java.security.MessageDigest
                .getInstance("MD5");
        digest.update(input.getBytes());
        byte messageDigest[] = digest.digest();

        // Create Hex String
        StringBuffer hexString = new StringBuffer();
        for (int i = 0; i < messageDigest.length; i++) {
            int var = 0xFF & messageDigest[i];
            if (var < 16)
                hexString.append("0");
            hexString.append(Integer.toHexString(var));
        }

        return hexString.toString();
    }
}
