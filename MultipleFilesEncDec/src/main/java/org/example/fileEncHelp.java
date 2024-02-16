package org.example;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class fileEncHelp {

    private static final String ENCRYPTION_ALGORITHM = "AES";
    private static final String SECRET_KEY = "MySecretKey12345"; // Replace with your secret key

    public static void main(String[] args) {
        String inputDirectory = "C:\\Users\\Kamal199261\\Documents\\csv_files";
        String outputDirectory = "C:\\Users\\Kamal199261\\Documents\\csv_enc_mul_files";

        // Encrypt files
        encryptFiles(inputDirectory, outputDirectory);

        // Decrypt files
        decryptFiles(outputDirectory);
    }

    private static void encryptFiles(String inputDirectory, String outputDirectory) {
        try {
            File[] files = new File(inputDirectory).listFiles();
            if (files != null) {
                for (File file : files) {
                    byte[] fileContent = Files.readAllBytes(file.toPath());
                    byte[] encryptedContent = encrypt(fileContent);
                    saveToFile(encryptedContent, outputDirectory + File.separator + file.getName());
                    System.out.println("File encrypted: " + file.getName());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void decryptFiles(String outputDirectory) {
        try {
            File[] files = new File(outputDirectory).listFiles();
            if (files != null) {
                for (File file : files) {
                    byte[] fileContent = Files.readAllBytes(file.toPath());
                    byte[] decryptedContent = decrypt(fileContent);
                    String decryptedString = new String(decryptedContent, StandardCharsets.UTF_8);
                    System.out.println("Decrypted content as String: " + "\n" + decryptedString);
                    System.out.println("Decrypted content as String array: " + "\n" + Arrays.toString(decryptedString.split("\\s+")));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static byte[] encrypt(byte[] content) {
        try {
            Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
            SecretKeySpec secretKeySpec = new SecretKeySpec(SECRET_KEY.getBytes(), ENCRYPTION_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
            return cipher.doFinal(content);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static byte[] decrypt(byte[] content) {
        try {
            Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
            SecretKeySpec secretKeySpec = new SecretKeySpec(SECRET_KEY.getBytes(), ENCRYPTION_ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
            return cipher.doFinal(content);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void saveToFile(byte[] content, String filePath) {
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            fos.write(content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
