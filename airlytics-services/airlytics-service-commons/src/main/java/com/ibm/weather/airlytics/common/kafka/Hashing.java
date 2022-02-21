package com.ibm.weather.airlytics.common.kafka;

import com.sangupta.murmur.Murmur2;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hashing {

    private int numberOfShards;
    MessageDigest md5;
    MessageDigest sha256;

    public Hashing(int numberOfShards) {
        this.numberOfShards = numberOfShards;
        try {
            md5 = MessageDigest.getInstance("MD5");
            sha256 = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            //TODO
            throw new RuntimeException(e);
        }
    }

    public int hashMD5(String userId)
    {
        byte[] messageDigest = md5.digest(userId.getBytes());

        // Convert byte array into signum representation
        BigInteger no = new BigInteger(1, messageDigest);
        return no.mod(BigInteger.valueOf(numberOfShards)).intValue();
    }

    public int hashSHA256(String userId)
    {
        byte[] messageDigest = sha256.digest(userId.getBytes());

        // Convert byte array into signum representation
        BigInteger no = new BigInteger(1, messageDigest);
        return no.mod(BigInteger.valueOf(numberOfShards)).intValue();
    }

    public int hashMurmur2(String userId)
    {
        byte[] value = userId.getBytes();
        Long no = Murmur2.hash(value, value.length, 894157739);
        return ((Long)(no%numberOfShards)).intValue();
    }
}
