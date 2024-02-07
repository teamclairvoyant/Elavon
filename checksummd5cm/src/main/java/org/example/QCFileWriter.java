package org.example;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class QCFileWriter {

    public static void main(String[] args) {
        String filePath = "C:\\Users\\Shridhar179377\\IdeaProjects\\checksummd5cm\\src\\main\\resources\\purchase_data.csv"; // Replace this with the path to your file
        String qcFileName = "example.txt"; // Replace this with the desired QC file name

        try {
            // Calculate checksum, record count, and file name
            String checksum = calculateMD5Checksum(filePath);
            int recordCount = getRecordCount(filePath);
            String fileName = getFileName(filePath);

            // Write to QC file
            writeQCFile(qcFileName, fileName, recordCount, checksum);

            System.out.println("QC file generated: " + qcFileName);
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public static String calculateMD5Checksum(String filePath) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        try (InputStream is = new FileInputStream(filePath)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = is.read(buffer)) > 0) {
                md.update(buffer, 0, read);
            }
        }
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    public static int getRecordCount(String filePath) throws IOException {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            while (reader.readLine() != null) {
                count++;
            }
        }
        return count-1;
    }

    public static String getFileName(String filePath) {
        File file = new File(filePath);
        return file.getName();
    }

    public static void writeQCFile(String qcFileName, String fileName, int recordCount, String checksum) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(qcFileName))) {
            writer.write(fileName + "|" + recordCount + "|" + checksum);
        }
    }
}
