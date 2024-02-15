package com.poc.processdata.azure;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;

@Slf4j
@Component
public class AzureADLSPush {

    @Value("${spring.azure.adls.containerName}")
    private String containerName;

    @Value("${spring.azure.adls.connectionString}")
    private String connectionString;

    @Value("${spring.azure.adls.filePath}")
    private String adlsFilePath;

    @Value("${spring.batch.file.result}")
    private String resultPath;

    @Value("${spring.batch.file.filePath}")
    private String filePath;

    public void pushToADLS() {
        DataLakeServiceClientBuilder serviceClientBuilder = new DataLakeServiceClientBuilder()
                .connectionString(connectionString);
        DataLakeFileSystemClient fileSystemClient = serviceClientBuilder
                .buildClient()
                .getFileSystemClient(containerName);
        File folder = new File(resultPath);
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                String destinationFilePath = adlsFilePath + file.getName();
                DataLakeFileClient fileClient = fileSystemClient.getFileClient(destinationFilePath);
                fileClient.uploadFromFile(file.getPath(), true);
                log.info("File uploaded successfully: " + file.getName());
            }
        } else {
            log.info("No files found in the local folder.");
        }
        log.info("File uploaded successfully to ADLS Gen2.");
    }

}
