package com.ibm.airlytics.retentiontrackerqueryhandler.utils;

import org.apache.commons.lang3.StringUtils;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.Provider;
import java.security.SecureRandom;
import java.util.UUID;


public final class AesGcmEncryption {
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    public static final int IV_LENGTH_BYTE = 12;
    private static final int TAG_LENGTH_BIT = 128;

    private final SecureRandom secureRandom;
    private final Provider provider;
    private ThreadLocal<Cipher> cipherWrapper = new ThreadLocal<>();

    public static byte[] uuidToBytes(String guid) {
        if (StringUtils.isEmpty(guid)) {
            return null;
        }
        UUID airlockUserId = UUID.fromString(guid);
        return uuidToBytes(airlockUserId);
    }

    public static byte[] uuidToBytes(UUID airlockUserId) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(airlockUserId.getMostSignificantBits());
        byteBuffer.putLong(airlockUserId.getLeastSignificantBits());
        return byteBuffer.array();
    }

    public static UUID bytesToUuid(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long firstLong = bb.getLong();
        long secondLong = bb.getLong();
        return new UUID(firstLong, secondLong);
    }

    public AesGcmEncryption() {
        this(new SecureRandom(), null);
    }

    public AesGcmEncryption(SecureRandom secureRandom) {
        this(secureRandom, null);
    }

    public AesGcmEncryption(SecureRandom secureRandom, Provider provider) {
        this.secureRandom = secureRandom;
        this.provider = provider;
    }

    public byte[] encrypt(byte[] rawEncryptionKey, byte[] rawData, boolean addIVLength) throws EncryptionException {
        if (rawEncryptionKey.length < 16) {
            throw new IllegalArgumentException("key length must be longer than 16 bytes");
        }

        byte[] iv = null;
        byte[] encrypted = null;
        try {
            iv = new byte[IV_LENGTH_BYTE];
            secureRandom.nextBytes(iv);

            final Cipher cipherEnc = getCipher();
            cipherEnc.init(
                    Cipher.ENCRYPT_MODE,
                    new SecretKeySpec(rawEncryptionKey, "AES"),
                    new GCMParameterSpec(TAG_LENGTH_BIT, iv));

            encrypted = cipherEnc.doFinal(rawData);

            if(addIVLength) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(1 + iv.length + encrypted.length);
                byteBuffer.put((byte) iv.length);
                byteBuffer.put(iv);
                byteBuffer.put(encrypted);
                return byteBuffer.array();
            } else {
                ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + encrypted.length);
                byteBuffer.put(iv);
                byteBuffer.put(encrypted);
                return byteBuffer.array();
            }
        } catch (Exception e) {
            throw new EncryptionException("could not encrypt", e);
        }
    }

    public byte[] decrypt(byte[] rawEncryptionKey, byte[] encryptedData, boolean hasIVLength) throws EncryptionException {
        try {

            int initialOffset = 0;
            int ivLength = IV_LENGTH_BYTE;

            if(hasIVLength) {
                initialOffset = 1;
                ivLength = encryptedData[0];

                if (ivLength != 12 && ivLength != 16) {
                    throw new IllegalStateException("Unexpected iv length");
                }
            }

            final Cipher cipherDec = getCipher();
            cipherDec.init(
                    Cipher.DECRYPT_MODE,
                    new SecretKeySpec(rawEncryptionKey, "AES"),
                    new GCMParameterSpec(TAG_LENGTH_BIT, encryptedData, initialOffset, ivLength));

            return cipherDec.doFinal(encryptedData, initialOffset + ivLength, encryptedData.length - (initialOffset + ivLength));
        } catch (Exception e) {
            throw new EncryptionException("could not decrypt", e);
        }
    }

    private Cipher getCipher() {
        Cipher cipher = cipherWrapper.get();
        if (cipher == null) {
            try {
                if (provider != null) {
                    cipher = Cipher.getInstance(ALGORITHM, provider);
                } else {
                    cipher = Cipher.getInstance(ALGORITHM);
                }
            } catch (Exception e) {
                throw new IllegalStateException("could not get cipher instance", e);
            }
            cipherWrapper.set(cipher);
            return cipherWrapper.get();
        } else {
            return cipher;
        }
    }
}

