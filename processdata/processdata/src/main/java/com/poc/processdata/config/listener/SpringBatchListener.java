package com.poc.processdata.config.listener;

import com.poc.processdata.azure.AzureADLSPush;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Slf4j
@RequiredArgsConstructor
@Component
public class SpringBatchListener implements JobExecutionListener {

    @Value("${spring.batch.file.SECRET_KEY}")
    private String secretKey;
    @Value("${spring.batch.file.result}")
    private String resultPath;
    @Value("${spring.batch.file.filePath}")
    private String filePath;
    /*
    decrypted file path
     */
    @Value("${spring.batch.file.decryptedFilePath}")
    private String decryptedFilePath;
    @Value("${spring.batch.file.ALGORITHM}")
    private String algorithm;
    @Value("${spring.batch.file.TRANSFORMATION}")
    private String transformation;
    /*
     pushing data to ADLS
     */
    private final AzureADLSPush azureADLSPush;

    private long startTime;

    /**
     * Retrieves and returns the record count from the specified file path.
     *
     * @param filePath The path of the file for which the record count is retrieved.
     * @return The count of records in the file.
     * @throws IOException If an I/O error occurs while reading the file.
     */

    public static int getRecordCount(String filePath) throws IOException {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            while (null != reader.readLine()) {
                count++;
            }
        }
        return count - 1;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        startTime = System.currentTimeMillis();
        long timesec = startTime;
        log.info("Job started at: " + timesec);
        decrypt();
    }

    /*
    Decrypts the file specified by 'filePath' using the provided SECRET_KEY and writes the decrypted content to 'decryptedFilePath'.
    Implementation details for file decryption using a specified algorithm and key
        */
    public void decrypt() {
        try {
            SecretKey secretKeySpec = new SecretKeySpec(this.secretKey.getBytes(), algorithm);
            Cipher cipher = Cipher.getInstance(transformation);
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
            File inputFile = new File(filePath);
            File outputFile = new File(decryptedFilePath);
            try (InputStream inputStream = new FileInputStream(inputFile);
                 OutputStream outputStream = new FileOutputStream(outputFile);
                 CipherOutputStream cipherOutputStream = new CipherOutputStream(outputStream, cipher);
            ) {
                byte[] buffer = new byte[1024];
                int bytesRead = inputStream.read(buffer);
                while (bytesRead >= 0) {
                    cipherOutputStream.write(buffer, 0, bytesRead);
                    bytesRead = inputStream.read(buffer);
                }
            }
        } catch (Exception e) {
            log.error("Error encrypting/decrypting file:", e);
        }
    }

    /*
    Implementation details for calculating checksum, record count, and file name, writing to QC file, pushing to Azure ADLS, and logging the generated QC file name
     */
    @Override
    public void afterJob(JobExecution jobExecution) {
        try {

            String checksum = calculateMD5Checksum(filePath);
            int recordCount = getRecordCount(filePath);
            String fileName = getFileName(filePath);
            String qcFileName = fileName.substring(0, fileName.indexOf('.')) + "-qc.txt";

            writeQCFile(qcFileName, fileName, recordCount, checksum);
            azureADLSPush.pushToADLS();
            log.info("QC file generated: " + qcFileName);
        } catch (IOException | NoSuchAlgorithmException e) {
            log.error("Error while generating QC file", e);
        }
    }

    /*
    /**
     * Calculates and returns the MD5 checksum for the specified file path.
     *
     * @param filePath The path of the file for which the MD5 checksum is calculated.
     * @return The MD5 checksum as a hexadecimal string.
     * @throws IOException              If an I/O error occurs while reading the file.
     * @throws NoSuchAlgorithmException If the MD5 algorithm is not available.
     */
    public String calculateMD5Checksum(String filePath) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        try (InputStream is = new FileInputStream(filePath)) {
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

    public String getFileName(String filePath) {
        File file = new File(filePath);
        return file.getName();
    }

    /**
     * Writes Quality Control information to a file, including file name, record count, and checksum.
     *
     * @param qcFileName  The name of the QC file to be written.
     * @param fileName    The original file name for which QC information is recorded.
     * @param recordCount The count of records in the file.
     * @param checksum    The checksum value of the file content.
     * @throws IOException If an I/O error occurs while writing the QC file.
     */

    public void writeQCFile(String qcFileName, String fileName, int recordCount, String checksum) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resultPath + "\\" + qcFileName))) {
            writer.write(fileName + "|" + recordCount + "|" + checksum);
        }
        long endTime = System.currentTimeMillis();
        log.info("Job finished at: " + endTime);

        long durationSeconds = (endTime - startTime) / 1000;
        log.info("Job duration: " + durationSeconds + " seconds");
    }


}
