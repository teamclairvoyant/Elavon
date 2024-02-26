package com.poc.processdata.helper;

import com.poc.processdata.azure.AzureADLSPush;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Component
@Slf4j
@RequiredArgsConstructor
public class QCFileHelper {

    @Value("${spring.batch.file.result}")
    private String resultPath;

    /*
    pushing data to ADLS
    */
    private final AzureADLSPush azureADLSPush;

    public void createQCFiles() {
        try {
            File directory = new File(resultPath);
            File[] files = directory.listFiles(pathname -> pathname.getName().endsWith(".csv"));
            if (null != files) {
                for (File file : files) {
                    String checksum = calculateMD5Checksum(file);
                    int recordCount = getRecordCount(file);
                    String fileName = file.getName();
                    String qcFileName = fileName.substring(0, fileName.indexOf('.')) + "-qc.txt";

                    writeQCFile(qcFileName, fileName, recordCount, checksum);
                    log.info("QC file generated: " + qcFileName);
                }
                azureADLSPush.pushToADLS();
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            log.error("Error while generating QC file", e);
        }
    }

    /**
     * Calculates and returns the MD5 checksum for the specified file path.
     *
     * @return The MD5 checksum as a hexadecimal string.
     * @throws IOException              If an I/O error occurs while reading the file.
     * @throws NoSuchAlgorithmException If the MD5 algorithm is not available.
     */
    private String calculateMD5Checksum(File file) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        try (InputStream is = new FileInputStream(file)) {
            byte[] buffer = new byte[8192];
            int read = is.read(buffer);
            while (read > 0) {
                md.update(buffer, 0, read);
                read = is.read(buffer);
            }
        }
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    /**
     * Retrieves and returns the record count from the specified file path.
     *
     * @return The count of records in the file.
     * @throws IOException If an I/O error occurs while reading the file.
     */

    private int getRecordCount(File file) throws IOException {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while (null != reader.readLine()) {
                count++;
            }
        }
        return count - 1;
    }

    private void writeQCFile(String qcFileName, String fileName, int recordCount, String checksum) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resultPath + "\\" + qcFileName))) {
            writer.write(fileName + "|" + recordCount + "|" + checksum);
        }
    }
}
