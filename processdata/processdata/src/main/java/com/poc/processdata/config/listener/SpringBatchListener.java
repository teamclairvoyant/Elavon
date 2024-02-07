package com.poc.processdata.config.listener;

import com.poc.processdata.AzureADLSPush;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SpringBatchListener implements JobExecutionListener {

    @Value("${spring.batch.file.result}")
    private String resultPath;

    @Value("${spring.batch.file.filePath}")
    private String filePath;

    @Autowired
    private AzureADLSPush azureADLSPush;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("before job");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        try {
// Calculate checksum, record count, and file name
            String checksum = calculateMD5Checksum(filePath);
            int recordCount = getRecordCount(filePath);
            String fileName = getFileName(filePath);
            String qcFileName = fileName.substring(0, fileName.indexOf(".")) + "-qc.txt";
// Write to QC file
            writeQCFile(qcFileName, fileName, recordCount, checksum);
            azureADLSPush.pushToADLS();
            System.out.println("QC file generated: " + qcFileName);
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public String calculateMD5Checksum(String filePath) throws IOException, NoSuchAlgorithmException {
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
        return count - 1;
    }

    public String getFileName(String filePath) {
        File file = new File(filePath);
        return file.getName();
    }

    public void writeQCFile(String qcFileName, String fileName, int recordCount, String checksum) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resultPath + "\\" + qcFileName))) {
            writer.write(fileName + "|" + recordCount + "|" + checksum);
        }
    }

}
