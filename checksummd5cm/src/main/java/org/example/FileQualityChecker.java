package org.example;

//import java.io.*;
//import java.nio.charset.StandardCharsets;
//import java.security.MessageDigest;
//import java.security.NoSuchAlgorithmException;
//
//public class QCFileWriter {
//
//    public static void main(String[] args) {
//        String filePath = "C:\\Users\\Shridhar179377\\IdeaProjects\\checksummd5cm\\src\\main\\resources"; // Replace this with the path to your file
//        String qcFileName = "example.txt"; // Replace this with the desired QC file name
//
//        try {
//            // Calculate checksum, record count, and file name
//            String checksum = calculateMD5Checksum(filePath);
//            int recordCount = getRecordCount(filePath);
//            String fileName = getFileName(filePath);
//
//            // Write to QC file
//            writeQCFile(qcFileName, fileName, recordCount, checksum);
//
//            System.out.println("QC file generated: " + qcFileName);
//        } catch (IOException | NoSuchAlgorithmException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static String calculateMD5Checksum(String filePath) throws IOException, NoSuchAlgorithmException {
//        MessageDigest md = MessageDigest.getInstance("MD5");
//        try (InputStream is = new FileInputStream(filePath)) {
//            byte[] buffer = new byte[8192];
//            int read;
//            while ((read = is.read(buffer)) > 0) {
//                md.update(buffer, 0, read);
//            }
//        }
//        byte[] digest = md.digest();
//        StringBuilder sb = new StringBuilder();
//        for (byte b : digest) {
//            sb.append(String.format("%02x", b & 0xff));
//        }
//        return sb.toString();
//    }
//
//    public static int getRecordCount(String filePath) throws IOException {
//        int count = 0;
//        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
//            while (reader.readLine() != null) {
//                count++;
//            }
//        }
//        return count-1;
//    }
//
//    public static String getFileName(String filePath) {
//        File file = new File(filePath);
//        return file.getName();
//    }
//
//    public static void writeQCFile(String qcFileName, String fileName, int recordCount, String checksum) throws IOException {
//        try (BufferedWriter writer = new BufferedWriter(new FileWriter(qcFileName))) {
//            writer.write(fileName + "|" + recordCount + "|" + checksum);
//        }
//    }
//}


////import java.io.*;
////import java.security.*;
//
////    public class FileChecksum {
//
//        public static void main(String[] args) {
//            // Provide the directory path where your files are located
//            String directoryPath = "C:\\Users\\Shridhar179377\\IdeaProjects\\checksummd5cm\\src\\main\\resources";
//
//            // List all files in the directory
//            File directory = new File(directoryPath);
//            File[] files = directory.listFiles();
//
//            if (files != null) {
//                for (File file : files) {
//                    if (file.isFile()) {
//                        try {
//                            String checksum = getMD5Checksum(file);
//                            System.out.println("File: " + file.getName() + ", MD5: " + checksum);
//                        } catch (IOException | NoSuchAlgorithmException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            } else {
//                System.out.println("Directory does not exist or is not accessible.");
//            }
//        }
//
//        public static String getMD5Checksum(File file) throws IOException, NoSuchAlgorithmException {
//            byte[] buffer = new byte[8192];
//            int count;
//            MessageDigest digest = MessageDigest.getInstance("MD5");
//            try (InputStream is = new FileInputStream(file)) {
//                while ((count = is.read(buffer)) > 0) {
//                    digest.update(buffer, 0, count);
//                }
//            }
//            byte[] hash = digest.digest();
//            StringBuilder result = new StringBuilder();
//            for (byte b : hash) {
//                result.append(String.format("%02x", b));
//            }
//            return result.toString();
//        }
//    }


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

public class FileQualityChecker {

    public static void main(String[] args) {
        String directoryPath = "C:\\Users\\Shridhar179377\\IdeaProjects\\checksummd5cm\\src\\main\\resources";
        String outputPath = "C:\\Users\\Shridhar179377\\IdeaProjects\\checksummd5cm\\src\\main\\java\\output\\file.txt";

        // Get the list of files in the directory
        File directory = new File(directoryPath);
        File[] files = directory.listFiles();

        if (files != null) {
            List<String> results = new ArrayList<>();

            for (File file : files) {
                if (file.isFile()) {
                    try {
                        String fileName = file.getName();
                        String md5Checksum = calculateMD5(file);
                        long recordCount = countRecords(file);

                        String result = fileName + "|" + md5Checksum + "|" + recordCount;
                        results.add(result);
                    } catch (IOException | NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
            }

            // Write results to file
            writeResults(outputPath, results);
        } else {
            System.err.println("No files found in the directory.");
        }
    }

    private static String calculateMD5(File file) throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        FileInputStream fis = new FileInputStream(file);

        byte[] byteArray = new byte[1024];
        int bytesCount;

        while ((bytesCount = fis.read(byteArray)) != -1) {
            digest.update(byteArray, 0, bytesCount);
        }

        fis.close();

        byte[] bytes = digest.digest();

        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
        }

        return sb.toString();
    }

    private static long countRecords(File file) throws IOException {
        // You need to implement this method to count records in the file
        // For demonstration purposes, let's assume it returns the number of lines in the file
        BufferedReader reader = new BufferedReader(new FileReader(file));
        long count = reader.lines().count();
        reader.close();
        return count;
    }

    private static void writeResults(String outputPath, List<String> results) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            for (String result : results) {
                writer.write(result);
                writer.newLine();
            }
            System.out.println("Results have been written to: " + outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
