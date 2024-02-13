package org.example;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileEncryptDecrypt {
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES";

    private static final String seceretkey = "MySecretKey12345";

    public static void encrypt(File inputFile, File outputFile) throws Exception {
        doCrypto(Cipher.ENCRYPT_MODE, inputFile, outputFile);
    }

    public static void decrypt(File inputFile, File outputFile) throws Exception {
        doCrypto(Cipher.DECRYPT_MODE, inputFile, outputFile);
    }

    private static void doCrypto(int cipherMode, File inputFile, File outputFile) throws Exception {
        try {
            SecretKey secretKey = new SecretKeySpec(seceretkey.getBytes(), ALGORITHM);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(cipherMode, secretKey);

            try (InputStream inputStream = new FileInputStream(inputFile);
                 OutputStream outputStream = new FileOutputStream(outputFile);
                 CipherOutputStream cipherOutputStream = new CipherOutputStream(outputStream, cipher)) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) >= 0) {
                    cipherOutputStream.write(buffer, 0, bytesRead);
                }
            }
        } catch (Exception e) {
            throw new Exception("Error encrypting/decrypting file: " + e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        String inputFolderPath = "C:\\Users\\Kamal199261\\Documents\\csv_files";
        String encryptedFolderPath = "C:\\Users\\Kamal199261\\Documents\\csv_enc_mul_files";
        String decryptedFolderPath = "C:\\Users\\Kamal199261\\Documents\\csv_mul_dec_files";

        try {
            Path inputPath = Paths.get(inputFolderPath);
            Files.list(inputPath).forEach(file -> {
                try {
                    File outputFile = new File(encryptedFolderPath + File.separator + file.getFileName());
                    encrypt(file.toFile(), outputFile);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            Path encryptedPath = Paths.get(encryptedFolderPath);
            Files.list(encryptedPath).forEach(file -> {
                try {
                    File outputFile = new File(decryptedFolderPath + File.separator + file.getFileName());
                    decrypt(file.toFile(), outputFile);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            System.out.println("Encryption and decryption completed successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
